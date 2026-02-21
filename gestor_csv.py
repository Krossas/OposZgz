#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Gestor de persistencia basado en CSV (optimizado para Streamlit)
- Cache en memoria para evitar lecturas repetidas
- Pandas para operaciones eficientes
- Invalidación automática de caché al guardar
"""

import os
import csv
import pandas as pd
from datetime import datetime
from typing import List, Tuple, Optional
from models import Convocatoria, EstadoConvocatoria
from utils import parsear_fecha


class GestorCSV:
    """Gestor CSV con cache interno para Streamlit"""
    
    def __init__(self, csv_path: str = "convocatorias.csv"):
        self.csv_path = csv_path
        self._cache_df = None  # Cache de DataFrame
        self._cache_convocatorias = None  # Cache de objetos Convocatoria
        self.inicializar_archivo()
    
    def inicializar_archivo(self):
        """Crea el archivo CSV si no existe"""
        if not os.path.exists(self.csv_path):
            df = pd.DataFrame(columns=[
                'Nivel', 'Título', 'Turno', 'Nº Total de Plazas',
                'Estado', 'Inicio', 'Fin', 'URL', 'fecha_scraping'
            ])
            df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
            self._cache_df = None
            self._cache_convocatorias = None
    
    def _leer_csv_pandas(self) -> pd.DataFrame:
        """Lee CSV usando pandas (más eficiente que csv.DictReader)"""
        try:
            if os.path.exists(self.csv_path) and os.path.getsize(self.csv_path) > 0:
                return pd.read_csv(self.csv_path, encoding='utf-8-sig')
            else:
                return pd.DataFrame(columns=[
                    'Nivel', 'Título', 'Turno', 'Nº Total de Plazas',
                    'Estado', 'Inicio', 'Fin', 'URL', 'fecha_scraping'
                ])
        except Exception as e:
            print(f"Error leyendo CSV: {e}")
            return pd.DataFrame()
    
    def _invalidar_cache(self):
        """Invalida el caché cuando cambian datos"""
        self._cache_df = None
        self._cache_convocatorias = None
    
    def _df_a_convocatorias(self, df: pd.DataFrame) -> List[Convocatoria]:
        """Convierte DataFrame a lista de Convocatoria (más eficiente)"""
        convocatorias = []
        
        for _, row in df.iterrows():
            try:
                fecha_inicio = None
                fecha_fin = None
                
                if pd.notna(row.get('Inicio')) and row['Inicio'] != '-':
                    fecha_inicio = parsear_fecha(str(row['Inicio']))
                if pd.notna(row.get('Fin')) and row['Fin'] != '-':
                    fecha_fin = parsear_fecha(str(row['Fin']))
                
                num_plazas = None
                if pd.notna(row.get('Nº Total de Plazas')) and row['Nº Total de Plazas'] != '-':
                    try:
                        num_plazas = int(float(row['Nº Total de Plazas']))
                    except:
                        pass
                
                fecha_scraping = datetime.now()
                if pd.notna(row.get('fecha_scraping')):
                    try:
                        fecha_scraping = datetime.fromisoformat(str(row['fecha_scraping']))
                    except:
                        pass
                
                turno = row.get('Turno')
                if pd.isna(turno) or turno == '-':
                    turno = None
                else:
                    turno = str(turno)
                
                conv = Convocatoria(
                    titulo=str(row.get('Título', '')),
                    nivel=str(row.get('Nivel', '')),
                    fecha_inicio=fecha_inicio,
                    fecha_fin=fecha_fin,
                    url=str(row.get('URL', '')),
                    url_detalle=str(row.get('URL', '')),
                    fecha_scraping=fecha_scraping,
                    turno=turno,
                    num_plazas=num_plazas
                )
                
                convocatorias.append(conv)
            
            except Exception as e:
                continue
        
        return convocatorias
    
    def guardar_convocatoria(self, conv: Convocatoria) -> bool:
        """
        Guarda o actualiza una convocatoria en CSV (upsert)
        
        Args:
            conv: Objeto Convocatoria
            
        Returns:
            True si insertó, False si actualizó
        """
        df = self._leer_csv_pandas()
        
        # Buscar por URL
        existe = (df['URL'] == conv.url).any()
        
        nueva_fila = {
            'Nivel': conv.nivel,
            'Título': conv.titulo,
            'Turno': conv.turno if conv.turno else '-',
            'Nº Total de Plazas': conv.num_plazas if conv.num_plazas else '-',
            'Estado': conv.estado.value,
            'Inicio': conv.fecha_inicio.strftime('%d/%m/%Y') if conv.fecha_inicio else '-',
            'Fin': conv.fecha_fin.strftime('%d/%m/%Y') if conv.fecha_fin else '-',
            'URL': conv.url,
            'fecha_scraping': datetime.now().isoformat()
        }
        
        if existe:
            # Actualizar
            df.loc[df['URL'] == conv.url] = pd.Series(nueva_fila)
            df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
            self._invalidar_cache()
            return False
        else:
            # Insertar
            df = pd.concat([df, pd.DataFrame([nueva_fila])], ignore_index=True)
            df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
            self._invalidar_cache()
            return True
    
    def obtener_todas(self, solo_interes: bool = True) -> List[Convocatoria]:
        """
        Obtiene todas las convocatorias (con cache)
        
        Args:
            solo_interes: Si es True, solo retorna C1 y C2
            
        Returns:
            Lista de Convocatoria
        """
        # Usar cache si existe
        if self._cache_convocatorias is not None:
            if solo_interes:
                return [c for c in self._cache_convocatorias if c.nivel in ['C1', 'C2']]
            else:
                return self._cache_convocatorias
        
        # Leer CSV
        df = self._leer_csv_pandas()
        convocatorias = self._df_a_convocatorias(df)
        
        # Guardar en cache
        self._cache_convocatorias = convocatorias
        
        if solo_interes:
            return [c for c in convocatorias if c.nivel in ['C1', 'C2']]
        else:
            return convocatorias
    
    def obtener_abiertas(self) -> List[Convocatoria]:
        """Obtiene solo convocatorias abiertas"""
        todas = self.obtener_todas()
        return [c for c in todas if c.estado == EstadoConvocatoria.ABIERTA]
    
    def obtener_pendientes(self) -> List[Convocatoria]:
        """Obtiene solo convocatorias pendientes"""
        todas = self.obtener_todas()
        return [c for c in todas if c.estado == EstadoConvocatoria.PENDIENTE]
    
    def obtener_cerradas(self) -> List[Convocatoria]:
        """Obtiene solo convocatorias cerradas"""
        todas = self.obtener_todas()
        return [c for c in todas if c.estado == EstadoConvocatoria.CERRADA]
    
    def obtener_por_nivel(self, nivel: str) -> List[Convocatoria]:
        """Obtiene convocatorias por nivel"""
        todas = self.obtener_todas(solo_interes=False)
        return [c for c in todas if c.nivel == nivel]
    
    def importar_csv(self, csv_path: str) -> Tuple[int, int, int]:
        """
        Importa convocatorias desde un archivo CSV
        
        Args:
            csv_path: Ruta del archivo CSV
            
        Returns:
            Tupla (insertas, actualizadas, ignoradas)
        """
        insertas = 0
        actualizadas = 0
        ignoradas = 0
        
        try:
            df_importado = pd.read_csv(csv_path, encoding='utf-8-sig')
            df_existente = self._leer_csv_pandas()
            
            for _, row in df_importado.iterrows():
                try:
                    nivel = str(row.get('Nivel', '')).strip()
                    titulo = str(row.get('Título', '')).strip()
                    turno = str(row.get('Turno', '')).strip() if pd.notna(row.get('Turno')) else None
                    url = str(row.get('URL', '')).strip()
                    
                    if not titulo or not url or not nivel:
                        ignoradas += 1
                        continue
                    
                    num_plazas = None
                    plazas_str = str(row.get('Nº Total de Plazas', '')).strip()
                    if plazas_str and plazas_str != '-':
                        try:
                            num_plazas = int(float(plazas_str))
                        except:
                            pass
                    
                    fecha_inicio = None
                    fecha_fin = None
                    inicio_str = str(row.get('Inicio', '')).strip()
                    fin_str = str(row.get('Fin', '')).strip()
                    
                    if inicio_str and inicio_str != '-':
                        fecha_inicio = parsear_fecha(inicio_str)
                    if fin_str and fin_str != '-':
                        fecha_fin = parsear_fecha(fin_str)
                    
                    conv = Convocatoria(
                        titulo=titulo,
                        nivel=nivel,
                        fecha_inicio=fecha_inicio,
                        fecha_fin=fecha_fin,
                        url=url,
                        url_detalle=url,
                        fecha_scraping=datetime.now(),
                        turno=turno,
                        num_plazas=num_plazas
                    )
                    
                    existe = (df_existente['URL'] == url).any()
                    
                    if existe:
                        actualizadas += 1
                        self.guardar_convocatoria(conv)
                    else:
                        insertas += 1
                        self.guardar_convocatoria(conv)
                    
                except Exception as e:
                    ignoradas += 1
                    continue
            
            return (insertas, actualizadas, ignoradas)
        
        except Exception as e:
            print(f"Error importando CSV: {e}")
            return (0, 0, ignoradas)
    
    def exportar_csv(self, csv_path: str, solo_interes: bool = True):
        """
        Exporta convocatorias a archivo CSV
        
        Args:
            csv_path: Ruta del archivo CSV de salida
            solo_interes: Si es True, solo exporta C1 y C2
        """
        convocatorias = self.obtener_todas(solo_interes=solo_interes)
        
        data = []
        for conv in convocatorias:
            data.append({
                'Nivel': conv.nivel,
                'Título': conv.titulo,
                'Turno': conv.turno if conv.turno else '-',
                'Nº Total de Plazas': conv.num_plazas if conv.num_plazas else '-',
                'Estado': conv.estado.value,
                'Inicio': conv.fecha_inicio.strftime('%d/%m/%Y') if conv.fecha_inicio else '-',
                'Fin': conv.fecha_fin.strftime('%d/%m/%Y') if conv.fecha_fin else '-',
                'URL': conv.url_detalle
            })
        
        df = pd.DataFrame(data)
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
