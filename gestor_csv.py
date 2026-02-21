#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Gestor de persistencia basado en CSV (sin SQLite)
"""

import csv
import os
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
from models import Convocatoria, EstadoConvocatoria
from utils import parsear_fecha
from config import DB_PATH

class GestorCSV:
    """Gestor simple de persistencia basado en CSV"""
    
    def __init__(self, csv_path: str = "convocatorias.csv"):
        self.csv_path = csv_path
        self.inicializar_archivo()
    
    def inicializar_archivo(self):
        """Crea el archivo CSV si no existe"""
        if not os.path.exists(self.csv_path):
            with open(self.csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                fieldnames = ['Nivel', 'Título', 'Turno', 'Nº Total de Plazas', 
                             'Estado', 'Inicio', 'Fin', 'URL', 'fecha_scraping']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
    
    def guardar_convocatoria(self, conv: Convocatoria) -> bool:
        """
        Guarda o actualiza una convocatoria en CSV (upsert inteligente)
        
        Args:
            conv: Objeto Convocatoria
            
        Returns:
            True si insertó, False si actualizó
        """
        # Leer datos existentes
        convocatorias_existentes = self.obtener_todas(solo_interes=False)
        
        # Buscar si existe por URL
        indice_existente = None
        for i, c in enumerate(convocatorias_existentes):
            if c.url == conv.url:
                indice_existente = i
                break
        
        # Preparar fila nueva
        fila_nueva = {
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
        
        if indice_existente is not None:
            # Actualizar
            convocatorias_existentes[indice_existente] = conv
            self._escribir_csv(convocatorias_existentes)
            return False
        else:
            # Insertar
            convocatorias_existentes.append(conv)
            self._escribir_csv(convocatorias_existentes)
            return True
    
    def _escribir_csv(self, convocatorias: List[Convocatoria]):
        """Escribe todas las convocatorias al CSV"""
        with open(self.csv_path, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = ['Nivel', 'Título', 'Turno', 'Nº Total de Plazas', 
                         'Estado', 'Inicio', 'Fin', 'URL', 'fecha_scraping']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for conv in convocatorias:
                writer.writerow({
                    'Nivel': conv.nivel,
                    'Título': conv.titulo,
                    'Turno': conv.turno if conv.turno else '-',
                    'Nº Total de Plazas': conv.num_plazas if conv.num_plazas else '-',
                    'Estado': conv.estado.value,
                    'Inicio': conv.fecha_inicio.strftime('%d/%m/%Y') if conv.fecha_inicio else '-',
                    'Fin': conv.fecha_fin.strftime('%d/%m/%Y') if conv.fecha_fin else '-',
                    'URL': conv.url,
                    'fecha_scraping': conv.fecha_scraping.isoformat() if conv.fecha_scraping else ''
                })
    
    def obtener_todas(self, solo_interes: bool = True) -> List[Convocatoria]:
        """
        Obtiene todas las convocatorias del CSV
        
        Args:
            solo_interes: Si es True, solo retorna C1 y C2
            
        Returns:
            Lista de Convocatoria
        """
        convocatorias = []
        
        if not os.path.exists(self.csv_path):
            return convocatorias
        
        try:
            with open(self.csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    try:
                        # Parsear falchas
                        fecha_inicio = None
                        fecha_fin = None
                        
                        if row.get('Inicio') and row['Inicio'] != '-':
                            fecha_inicio = parsear_fecha(row['Inicio'])
                        if row.get('Fin') and row['Fin'] != '-':
                            fecha_fin = parsear_fecha(row['Fin'])
                        
                        # Parsear plazas
                        num_plazas = None
                        if row.get('Nº Total de Plazas') and row['Nº Total de Plazas'] != '-':
                            try:
                                num_plazas = int(row['Nº Total de Plazas'])
                            except:
                                pass
                        
                        # Parsear fecha scraping
                        fecha_scraping = datetime.now()
                        if row.get('fecha_scraping'):
                            try:
                                fecha_scraping = datetime.fromisoformat(row['fecha_scraping'])
                            except:
                                pass
                        
                        conv = Convocatoria(
                            titulo=row.get('Título', ''),
                            nivel=row.get('Nivel', ''),
                            fecha_inicio=fecha_inicio,
                            fecha_fin=fecha_fin,
                            url=row.get('URL', ''),
                            url_detalle=row.get('URL', ''),
                            fecha_scraping=fecha_scraping,
                            turno=row.get('Turno') if row.get('Turno') != '-' else None,
                            num_plazas=num_plazas
                        )
                        
                        if solo_interes:
                            if conv.nivel in ['C1', 'C2']:
                                convocatorias.append(conv)
                        else:
                            convocatorias.append(conv)
                    
                    except Exception as e:
                        continue
        
        except Exception as e:
            pass
        
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
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    try:
                        nivel = row.get('Nivel', '').strip()
                        titulo = row.get('Título', '').strip()
                        turno = row.get('Turno', '').strip() or None
                        plazas_str = row.get('Nº Total de Plazas', '').strip()
                        inicio_str = row.get('Inicio', '').strip()
                        fin_str = row.get('Fin', '').strip()
                        url = row.get('URL', '').strip()
                        
                        if not titulo or not url or not nivel:
                            ignoradas += 1
                            continue
                        
                        # Parsear número de plazas
                        num_plazas = None
                        if plazas_str and plazas_str != '-':
                            try:
                                import re
                                match = re.search(r'^(\d+)', plazas_str)
                                if match:
                                    num_plazas = int(match.group(1))
                            except:
                                pass
                        
                        # Parsear fechas
                        fecha_inicio = None
                        fecha_fin = None
                        if inicio_str and inicio_str != '-':
                            fecha_inicio = parsear_fecha(inicio_str)
                        if fin_str and fin_str != '-':
                            fecha_fin = parsear_fecha(fin_str)
                        
                        # Crear objeto Convocatoria
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
                        
                        # Verificar si existe
                        existe = any(c.url == url for c in self.obtener_todas(solo_interes=False))
                        
                        if existe:
                            # Actualizar
                            self.guardar_convocatoria(conv)
                            actualizadas += 1
                        else:
                            # Insertar
                            self.guardar_convocatoria(conv)
                            insertas += 1
                    
                    except Exception as e:
                        ignoradas += 1
                        continue
            
            return (insertas, actualizadas, ignoradas)
        
        except Exception as e:
            return (0, 0, ignoradas)
    
    def exportar_csv(self, csv_path: str, solo_interes: bool = True):
        """
        Exporta convocatorias a archivo CSV
        
        Args:
            csv_path: Ruta del archivo CSV de salida
            solo_interes: Si es True, solo exporta C1 y C2
        """
        convocatorias = self.obtener_todas(solo_interes=solo_interes)
        
        with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = ['Nivel', 'Título', 'Turno', 'Nº Total de Plazas', 'Estado', 'Inicio', 'Fin', 'URL']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            
            for conv in convocatorias:
                writer.writerow({
                    'Nivel': conv.nivel,
                    'Título': conv.titulo,
                    'Turno': conv.turno if conv.turno else '-',
                    'Nº Total de Plazas': conv.num_plazas if conv.num_plazas else '-',
                    'Estado': conv.estado.value,
                    'Inicio': conv.fecha_inicio.strftime('%d/%m/%Y') if conv.fecha_inicio else '-',
                    'Fin': conv.fecha_fin.strftime('%d/%m/%Y') if conv.fecha_fin else '-',
                    'URL': conv.url_detalle
                })
