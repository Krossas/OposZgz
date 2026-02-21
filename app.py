#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Interfaz Streamlit para gestión de oposiciones C1/C2 Zaragoza
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from database import DatabaseManager
from scraper import ScraperOposiciones
from models import EstadoConvocatoria
import logging

# Configuración Streamlit
st.set_page_config(
    page_title="Oposiciones C1/C2 Zaragoza",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personalizado
st.markdown("""
    <style>
    .main { padding: 2rem; }
    .metric { background-color: #f0f2f6; padding: 1rem; border-radius: 0.5rem; }
    </style>
    """, unsafe_allow_html=True)

# Título
st.title("📋 Gestor de Oposiciones C1/C2")
st.markdown("### Ayuntamiento de Zaragoza")

# Inicializar sesión
if 'db' not in st.session_state:
    st.session_state.db = DatabaseManager()

db = st.session_state.db

# ============================================================================
# SIDEBAR - CONTROLES PRINCIPALES
# ============================================================================
with st.sidebar:
    st.header("Controles")
    
    # Botón para ejecutar scraper
    if st.button("🔄 Actualizar desde Web", use_container_width=True):
        with st.spinner("Scrapeando... por favor espera..."):
            try:
                scraper = ScraperOposiciones()
                convocatorias = scraper.scraping_completo()
                scraper.close()
                
                # Guardar en BD (con upsert inteligente)
                nuevas = 0
                actualizadas = 0
                for conv in convocatorias:
                    db.guardar_convocatoria(conv)
                    nuevas += 1
                
                st.success(f"✓ {len(convocatorias)} convocatorias procesadas (nuevas + actualizadas)")
                st.balloons()
            except Exception as e:
                st.error(f"Error en scraping: {str(e)}")
    
    st.divider()
    
    # Importar CSV
    st.subheader("Importar desde CSV")
    uploaded_file = st.file_uploader("Selecciona archivo CSV", type=["csv"])
    
    if uploaded_file is not None:
        if st.button("📥 Importar CSV", use_container_width=True):
            with st.spinner("Importando..."):
                try:
                    import tempfile
                    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                        tmp.write(uploaded_file.getbuffer())
                        tmp_path = tmp.name
                    
                    insertas, actualizadas, ignoradas = db.importar_csv(tmp_path)
                    
                    st.success(f"✓ Importación completada")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Insertadas", insertas)
                    col2.metric("Actualizadas", actualizadas)
                    col3.metric("Ignoradas", ignoradas)
                    
                    import os
                    os.unlink(tmp_path)
                    
                    # Recargar datos en sesión
                    st.session_state.refresh = True
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"Error al importar: {str(e)}")
    
    st.divider()
    
    # Filtros
    st.subheader("Filtros")
    
    # Nivel
    nivel_filter = st.multiselect(
        "Nivel",
        options=["C1", "C2"],
        default=["C1", "C2"],
        key="nivel_filter"
    )
    
    # Estado
    estado_filter = st.multiselect(
        "Estado",
        options=["ABIERTA", "PENDIENTE", "CERRADA"],
        default=["ABIERTA", "PENDIENTE", "CERRADA"],
        key="estado_filter"
    )
    
    st.divider()
    
    # Información
    st.subheader("Información")
    todas = db.obtener_todas()
    st.metric(label="Total en BD", value=len(todas))
    
    abiertas = db.obtener_abiertas()
    st.metric(label="Convocatorias Abiertas", value=len(abiertas))
    
    st.divider()
    
    # Última actualización
    if todas:
        max_fecha = max([c.fecha_scraping for c in todas])
        st.caption(f"Última actualización: {max_fecha.strftime('%d/%m/%Y %H:%M')}")

# ============================================================================
# CONTENIDO PRINCIPAL - TABLA DE CONVOCATORIAS
# ============================================================================

# Obtener datos
todas = db.obtener_todas()

# Filtrar
convocatorias_filtradas = [
    c for c in todas 
    if c.nivel in nivel_filter and c.estado.value in estado_filter
]

st.header(f"Convocatorias ({len(convocatorias_filtradas)} de {len(todas)})")

if convocatorias_filtradas:
    # Convertir a DataFrame (sin URL visible)
    data = []
    for idx, c in enumerate(convocatorias_filtradas, 1):
        data.append({
            "#": idx,
            "Nivel": c.nivel,
            "Título": c.titulo[:50] + "..." if len(c.titulo) > 50 else c.titulo,
            "Turno": c.turno if c.turno else "-",
            "Plazas": c.num_plazas if c.num_plazas else "-",
            "Estado": f"🟢 {c.estado.value}" if c.estado == EstadoConvocatoria.ABIERTA 
                     else f"🟡 {c.estado.value}" if c.estado == EstadoConvocatoria.PENDIENTE
                     else f"🔴 {c.estado.value}",
            "Inicio": c.fecha_inicio.strftime("%d/%m/%Y") if c.fecha_inicio else "-",
            "Fin": c.fecha_fin.strftime("%d/%m/%Y") if c.fecha_fin else "-",
            "Díás Restantes": (c.fecha_fin - datetime.now()).days if c.fecha_fin else 0
        })
    
    df = pd.DataFrame(data)
    
    # Mostrar tabla
    st.dataframe(
        df,
        use_container_width=True,
        height=400,
        hide_index=True
    )
    
    st.divider()
    
    # Sección de URLs - Expandible para cada convocatoria
    st.subheader("Enlaces a Convocatorias")
    
    cols = st.columns([2, 1, 1])
    with cols[0]:
        st.caption("Selecciona una convocatoria para ver y copiar su URL")
    
    # Crear selectbox con títulos
    titulos = [f"{idx}. {c.titulo[:60]}" for idx, c in enumerate(convocatorias_filtradas, 1)]
    selected_idx = st.selectbox("", titulos, key="url_selector", label_visibility="collapsed")
    
    if selected_idx:
        idx = int(selected_idx.split(".")[0]) - 1
        conv_seleccionada = convocatorias_filtradas[idx]
        
        # Mostrar URL completa
        st.write("**URL Completa:**")
        
        col1, col2 = st.columns([4, 1])
        with col1:
            st.code(conv_seleccionada.url_detalle, language=None)
        
        with col2:
            # Botón para copiar
            if st.button("📋 Copiar", key=f"copy_{idx}", use_container_width=True):
                st.write(f"```\n{conv_seleccionada.url_detalle}\n```")
                st.info("URL copiada a portapapeles. Puedes pegarla con Ctrl+V / Cmd+V")
        
        # Botón para abrir en nueva pestaña
        st.markdown(
            f"""
            <a href="{conv_seleccionada.url_detalle}" target="_blank">
                <button style="
                    background-color: #4CAF50;
                    color: white;
                    padding: 10px 20px;
                    border: none;
                    border-radius: 4px;
                    cursor: pointer;
                    font-size: 14px;
                    width: 100%;
                    margin-top: 10px;
                ">🌐 Abrir en Nueva Pestaña</button>
            </a>
            """,
            unsafe_allow_html=True
        )
    
    st.divider()
    
    # Sección de exportación
    st.subheader("Exportar")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Exportar a CSV
        csv_data = df.to_csv(index=False).encode('utf-8-sig')  # UTF-8 con BOM para Excel
        st.download_button(
            label="📥 Descargar CSV",
            data=csv_data,
            file_name=f"oposiciones_c1c2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    st.divider()
    
    # Estadísticas
    st.subheader("Estadísticas")
    
    col1, col2, col3 = st.columns(3)
    
    c1_count = len([c for c in convocatorias_filtradas if c.nivel == "C1"])
    c2_count = len([c for c in convocatorias_filtradas if c.nivel == "C2"])
    
    with col1:
        st.metric("C1", c1_count)
    
    with col2:
        st.metric("C2", c2_count)
    
    with col3:
        abierta_count = len([c for c in convocatorias_filtradas if c.estado == EstadoConvocatoria.ABIERTA])
        st.metric("Abiertas", abierta_count)

else:
    st.info("No hay convocatorias con los filtros seleccionados.")

# ============================================================================
# FOOTER
# ============================================================================
st.divider()
st.caption("Datos actualizados desde: https://www.zaragoza.es/oferta/")
st.caption("Desarrollado con Streamlit | Python")
