import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
import csv
from models import Convocatoria, EstadoConvocatoria
from config import DB_PATH, NIVELES_INTERES
from utils import parsear_fecha

class DatabaseManager:
    """Gestor de base de datos SQLite para convocatorias"""
    
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.inicializar_bd()
    
    def inicializar_bd(self):
        """Crea las tablas si no existen"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Tabla principal de convocatorias
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS convocatorias (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                titulo TEXT NOT NULL,
                nivel TEXT NOT NULL,
                turno TEXT,
                num_plazas INTEGER,
                fecha_inicio TEXT,
                fecha_fin TEXT,
                url TEXT NOT NULL UNIQUE,
                url_detalle TEXT,
                fecha_scraping TEXT NOT NULL,
                estado TEXT NOT NULL,
                fecha_creacion TEXT NOT NULL,
                fecha_actualizacion TEXT NOT NULL
            )
            """)
            
            # Tabla histórica para auditoría
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS historico (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_convocatoria INTEGER,
                estado_anterior TEXT,
                estado_nuevo TEXT,
                fecha_cambio TEXT,
                FOREIGN KEY (id_convocatoria) REFERENCES convocatorias(id)
            )
            """)
            
            conn.commit()
    
    def guardar_convocatoria(self, conv: Convocatoria) -> int:
        """
        Guarda o actualiza una convocatoria
        
        Args:
            conv: Objeto Convocatoria
            
        Returns:
            ID de la convocatoria
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Verificar si existe
            cursor.execute("SELECT id, estado FROM convocatorias WHERE url = ?", (conv.url,))
            resultado = cursor.fetchone()
            
            ahora = datetime.now().isoformat()
            
            if resultado:
                # Actualizar
                id_conv = resultado['id']
                estado_anterior = resultado['estado']
                
                cursor.execute("""
                UPDATE convocatorias 
                SET titulo = ?, nivel = ?, turno = ?, num_plazas = ?, 
                    fecha_inicio = ?, fecha_fin = ?, url_detalle = ?, 
                    fecha_scraping = ?, estado = ?, fecha_actualizacion = ?
                WHERE id = ?
                """, (
                    conv.titulo,
                    conv.nivel,
                    conv.turno,
                    conv.num_plazas,
                    conv.fecha_inicio.isoformat() if conv.fecha_inicio else None,
                    conv.fecha_fin.isoformat() if conv.fecha_fin else None,
                    conv.url_detalle,
                    conv.fecha_scraping.isoformat(),
                    conv.estado.value,
                    ahora,
                    id_conv
                ))
                
                # Registrar cambio de estado si aplica
                if estado_anterior != conv.estado.value:
                    cursor.execute("""
                    INSERT INTO historico (id_convocatoria, estado_anterior, estado_nuevo, fecha_cambio)
                    VALUES (?, ?, ?, ?)
                    """, (id_conv, estado_anterior, conv.estado.value, ahora))
            else:
                # Insertar
                cursor.execute("""
                INSERT INTO convocatorias 
                (titulo, nivel, turno, num_plazas, fecha_inicio, fecha_fin, 
                 url, url_detalle, fecha_scraping, estado, fecha_creacion, fecha_actualizacion)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    conv.titulo,
                    conv.nivel,
                    conv.turno,
                    conv.num_plazas,
                    conv.fecha_inicio.isoformat() if conv.fecha_inicio else None,
                    conv.fecha_fin.isoformat() if conv.fecha_fin else None,
                    conv.url,
                    conv.url_detalle,
                    conv.fecha_scraping.isoformat(),
                    conv.estado.value,
                    ahora,
                    ahora
                ))
                
                id_conv = cursor.lastrowid
            
            conn.commit()
            return id_conv
    
    def obtener_todas(self, solo_interes: bool = True) -> List[Convocatoria]:
        """
        Obtiene todas las convocatorias
        
        Args:
            solo_interes: Si es True, solo retorna C1 y C2
            
        Returns:
            Lista de Convocatoria
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if solo_interes:
                sql = "SELECT * FROM convocatorias WHERE nivel IN (?, ?) ORDER BY fecha_fin DESC"
                cursor.execute(sql, ("C1", "C2"))
            else:
                sql = "SELECT * FROM convocatorias ORDER BY fecha_fin DESC"
                cursor.execute(sql)
            
            resultados = []
            for row in cursor.fetchall():
                conv = self._row_to_convocatoria(row)
                resultados.append(conv)
            
            return resultados
    
    def obtener_por_estado(self, estado: EstadoConvocatoria) -> List[Convocatoria]:
        """
        Obtiene convocatorias filtradas por estado
        
        Args:
            estado: EstadoConvocatoria a filtrar
            
        Returns:
            Lista de Convocatoria
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            sql = """
            SELECT * FROM convocatorias 
            WHERE estado = ? AND nivel IN (?, ?)
            ORDER BY fecha_fin DESC
            """
            cursor.execute(sql, (estado.value, "C1", "C2"))
            
            resultados = []
            for row in cursor.fetchall():
                conv = self._row_to_convocatoria(row)
                resultados.append(conv)
            
            return resultados
    
    def obtener_por_nivel(self, nivel: str) -> List[Convocatoria]:
        """
        Obtiene convocatorias de un nivel específico
        
        Args:
            nivel: "C1" o "C2"
            
        Returns:
            Lista de Convocatoria
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            sql = "SELECT * FROM convocatorias WHERE nivel = ? ORDER BY fecha_fin DESC"
            cursor.execute(sql, (nivel,))
            
            resultados = []
            for row in cursor.fetchall():
                conv = self._row_to_convocatoria(row)
                resultados.append(conv)
            
            return resultados
    
    def obtener_abiertas(self) -> List[Convocatoria]:
        """Obtiene convocatorias actualmente abiertas"""
        return self.obtener_por_estado(EstadoConvocatoria.ABIERTA)
    
    def obtener_pendientes(self) -> List[Convocatoria]:
        """Obtiene convocatorias pendientes"""
        return self.obtener_por_estado(EstadoConvocatoria.PENDIENTE)
    
    def obtener_cerradas(self) -> List[Convocatoria]:
        """Obtiene convocatorias cerradas"""
        return self.obtener_por_estado(EstadoConvocatoria.CERRADA)
    
    def limpiar_antiguas(self, dias: int = 365):
        """
        Elimina convocatorias cerradas hace más de X días
        
        Args:
            dias: Días desde el cierre
        """
        from datetime import timedelta
        
        fecha_limite = (datetime.now() - timedelta(days=dias)).isoformat()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            DELETE FROM convocatorias 
            WHERE estado = ? AND fecha_actualizacion < ?
            """, (EstadoConvocatoria.CERRADA.value, fecha_limite))
            conn.commit()
            return cursor.rowcount
    
    def obtener_historial(self, id_convocatoria: int) -> List[tuple]:
        """Obtiene el historial de cambios de una convocatoria"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            sql = """
            SELECT estado_anterior, estado_nuevo, fecha_cambio 
            FROM historico 
            WHERE id_convocatoria = ? 
            ORDER BY fecha_cambio DESC
            """
            cursor.execute(sql, (id_convocatoria,))
            return cursor.fetchall()
    
    def exportar_csv(self, ruta_salida: Path):
        """Exporta convocatorias C1/C2 a CSV"""
        import csv
        
        convocatorias = self.obtener_todas()
        
        with open(ruta_salida, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Nivel', 'Título', 'Estado', 'Fecha Inicio', 'Fecha Fin', 
                'Días Restantes', 'URL', 'Last Updated'
            ])
            
            for conv in convocatorias:
                from utils import formatear_fecha, dias_hasta_fecha
                dias = dias_hasta_fecha(conv.fecha_fin)
                writer.writerow([
                    conv.nivel,
                    conv.titulo,
                    conv.estado.value,
                    formatear_fecha(conv.fecha_inicio),
                    formatear_fecha(conv.fecha_fin),
                    dias if dias is not None else "",
                    conv.url_detalle,
                    conv.fecha_scraping.strftime("%d/%m/%Y %H:%M:%S")
                ])
    
    @staticmethod
    def _row_to_convocatoria(row) -> Convocatoria:
        """Convierte una fila SQLite a objeto Convocatoria"""
        fecha_inicio = None
        fecha_fin = None
        
        if row['fecha_inicio']:
            fecha_inicio = datetime.fromisoformat(row['fecha_inicio'])
        if row['fecha_fin']:
            fecha_fin = datetime.fromisoformat(row['fecha_fin'])
        
        return Convocatoria(
            titulo=row['titulo'],
            nivel=row['nivel'],
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin,
            url=row['url'],
            url_detalle=row['url_detalle'],
            fecha_scraping=datetime.fromisoformat(row['fecha_scraping']),
            turno=row['turno'],
            num_plazas=row['num_plazas'],
            estado=EstadoConvocatoria(row['estado'])
        )
    
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
                        # El CSV debe tener: Nivel, Título, Turno, Nº Total de Plazas, Inicio, Fin, URL, Estado
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
                        if plazas_str:
                            try:
                                # Extraer solo el número (ej: "25" o "25   -   Para Promoción Interna (25)")
                                import re
                                match = re.search(r'^(\d+)', plazas_str)
                                if match:
                                    num_plazas = int(match.group(1))
                            except:
                                pass
                        
                        # Parsear fechas
                        fecha_inicio = parsear_fecha(inicio_str) if inicio_str and inicio_str != '-' else None
                        fecha_fin = parsear_fecha(fin_str) if fin_str and fin_str != '-' else None
                        
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
                        with sqlite3.connect(self.db_path) as conn:
                            cursor = conn.cursor()
                            cursor.execute("SELECT id FROM convocatorias WHERE url = ?", (url,))
                            existe = cursor.fetchone()
                        
                        if existe:
                            # Actualizar si cambió algo
                            id_conv = self.guardar_convocatoria(conv)
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
