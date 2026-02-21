import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Tuple, Set
from models import Convocatoria, EstadoConvocatoria
from utils import (
    parsear_fecha, extraer_fechas_texto, identificar_nivel, 
    es_nivel_interes, limpiar_texto
)
from config import ZARAGOZA_URL, REQUEST_TIMEOUT
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScraperOposiciones:
    """Scraper para convocatorias del Ayuntamiento de Zaragoza"""
    
    def __init__(self, url_base: str = ZARAGOZA_URL):
        self.url_base = url_base
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def obtener_listado_convocatorias(self) -> List[Tuple[str, str]]:
        """
        Obtiene el listado de convocatorias desde la página principal.
        Busca todas las URLs de ofertaDetalle.jsp?id= y sus títulos en el HTML usando regex.
        
        Returns:
            Lista de tuplas (título, url_detalle)
        """
        try:
            logger.info(f"Descargando pagina principal: {self.url_base}")
            response = self.session.get(self.url_base, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            html_content = response.text
            
            # Buscar todas las URLs de ofertaDetalle con sus títulos usando regex
            # Patrón: <a href="ofertaDetalle.jsp?id=XXXX">Título</a>
            pattern = r'<a\s+href="([^"]*ofertaDetalle\.jsp\?id=\d+)"[^>]*>([^<]+)</a>'
            matches = re.finditer(pattern, html_content, re.IGNORECASE)
            
            urls_unicas: Set[str] = set()  # Evitar duplicados
            convocatorias = []
            
            for match in matches:
                url_relativa = match.group(1)  # href
                titulo_raw = match.group(2)      # texto del link
                
                # Normalizar URL relativa a absoluta
                if url_relativa.startswith('/'):
                    url_absoluta = self.url_base.rstrip('/') + url_relativa
                elif url_relativa.startswith('http'):
                    url_absoluta = url_relativa
                else:
                    url_absoluta = self.url_base.rstrip('/') + '/' + url_relativa
                
                # Evitar duplicados
                if url_absoluta in urls_unicas:
                    continue
                urls_unicas.add(url_absoluta)
                
                # Limpiar título
                titulo = limpiar_texto(titulo_raw)
                
                if titulo:
                    convocatorias.append((titulo, url_absoluta))
                    logger.debug(f"Convocatoria: {titulo[:40]}... | URL: {url_absoluta[-40:]}")
            
            logger.info(f"Total de convocatorias encontradas: {len(convocatorias)}")
            return convocatorias
        
        except requests.RequestException as e:
            logger.error(f"Error descargando página: {e}")
            return []
        except Exception as e:
            logger.error(f"Error procesando listado: {e}")
            return []
    
    def obtener_detalle_convocatoria(self, url: str) -> Convocatoria or None:
        """
        Obtiene los detalles de una convocatoria individual
        
        Args:
            url: URL de la convocatoria
            
        Returns:
            Objeto Convocatoria o None si hay error
        """
        try:
            logger.info(f"Scrapeando detalle: {url[:60]}...")
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extraer título
            titulo_elem = soup.find('h2')
            titulo = limpiar_texto(titulo_elem.get_text()) if titulo_elem else "Sin título"
            
            # Extraer todo el contenido de texto
            contenido = soup.get_text()
            
            # Extraer Turno
            turno = None
            turno_dt = soup.find('dt', class_='separador', string=lambda x: x and 'Turno' in x)
            if turno_dt:
                turno_dd = turno_dt.find_next('dd')
                if turno_dd:
                    turno = limpiar_texto(turno_dd.get_text())
            
            # Extraer Nº Total de Plazas
            num_plazas = None
            plazas_dt = soup.find('dt', class_='separador', string=lambda x: x and 'Plazas' in x)
            if plazas_dt:
                plazas_dd = plazas_dt.find_next('dd')
                if plazas_dd:
                    plazas_text = limpiar_texto(plazas_dd.get_text())
                    # Extraer el primer número
                    import re
                    match = re.search(r'^(\d+)', plazas_text)
                    if match:
                        try:
                            num_plazas = int(match.group(1))
                        except:
                            pass
            
            # Identificar nivel
            nivel = identificar_nivel(titulo, contenido)
            
            # Si no es de interés, retornar None
            if not es_nivel_interes(nivel):
                logger.debug(f"Nivel no de interes ({nivel}): {titulo}")
                return None
            
            logger.info(f"Nivel encontrado: {nivel}")
            
            # Extraer fechas de "Presentación de instancias"
            fecha_inicio = None
            fecha_fin = None
            
            # Buscar el dt con "Presentación de instancias"
            dt_presentacion = soup.find('dt', string=lambda x: x and 'Presentación de instancias' in x)
            
            if not dt_presentacion:
                # Alternativamente, buscar en todo el dt (puede tener span adentro)
                all_dts = soup.find_all('dt')
                for dt in all_dts:
                    dt_text = dt.get_text()
                    if 'Presentación de instancias' in dt_text:
                        dt_presentacion = dt
                        break
            
            if dt_presentacion:
                # Extraer el texto completo del dt
                dt_text = dt_presentacion.get_text()
                logger.debug(f"Texto dt presentación: {dt_text}")
                
                # Buscar patrón "plazo del DD/MM/YYYY al DD/MM/YYYY"
                # Captura ambas fechas
                patron_fechas = r'plazo\s+del\s+(\d{1,2}/\d{1,2}/\d{4})\s+al\s+(\d{1,2}/\d{1,2}/\d{4})'
                match = re.search(patron_fechas, dt_text, re.IGNORECASE)
                
                if match:
                    fecha_inicio_str = match.group(1)  # First date
                    fecha_fin_str = match.group(2)     # Second date
                    
                    fecha_inicio = parsear_fecha(fecha_inicio_str)
                    fecha_fin = parsear_fecha(fecha_fin_str)
                    
                    logger.debug(f"Fechas extraídas: {fecha_inicio_str} al {fecha_fin_str}")
            
            # Si no encontró fechas, intentar extraer de otros elementos (fallback)
            if not fecha_inicio or not fecha_fin:
                textos = [elem.get_text() for elem in soup.find_all(['p', 'div', 'span', 'td', 'tr'])]
                fechas_encontradas = []
                
                for texto in textos:
                    fechas = extraer_fechas_texto(texto)
                    for fecha_str, fecha_obj in fechas:
                        if fecha_obj and fecha_obj not in fechas_encontradas:
                            fechas_encontradas.append(fecha_obj)
                
                # Usar la menor como inicio y mayor como fin
                if fechas_encontradas:
                    fechas_encontradas.sort()
                    fecha_inicio = fechas_encontradas[0]
                    fecha_fin = fechas_encontradas[-1] if len(fechas_encontradas) > 1 else fechas_encontradas[0]
            
            # Crear objeto Convocatoria
            convocatoria = Convocatoria(
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
            
            logger.debug(f"Convocatoria procesada: {nivel} - {titulo[:40]}...")
            return convocatoria
        
        except requests.RequestException as e:
            logger.error(f"Error descargando detalle: {e}")
            return None
        except Exception as e:
            logger.error(f"Error procesando detalle: {e}")
            return None
    
    def scraping_completo(self) -> List[Convocatoria]:
        """
        Realiza scraping completo: listado + detalles
        
        Returns:
            Lista de Convocatoria
        """
        logger.info("Iniciando scraping completo...")
        
        listado = self.obtener_listado_convocatorias()
        logger.info(f"Total de elementos encontrados: {len(listado)}")
        
        convocatorias = []
        
        for i, (titulo, url) in enumerate(listado, 1):
            logger.info(f"Procesando {i}/{len(listado)}")
            
            conv = self.obtener_detalle_convocatoria(url)
            if conv:
                convocatorias.append(conv)
        
        logger.info(f"Scraping completado. Total de C1/C2 encontradas: {len(convocatorias)}")
        return convocatorias
    
    def close(self):
        """Cierra la sesión"""
        self.session.close()
