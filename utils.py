from datetime import datetime
import re
from typing import Optional
from config import FORMATO_FECHAS, KEYWORDS_C1, KEYWORDS_C2, NIVELES_INTERES

def parsear_fecha(texto: str) -> Optional[datetime]:
    """
    Intenta parsear una fecha desde texto con múltiples formatos
    
    Args:
        texto: Cadena que contiene la fecha
        
    Returns:
        datetime si se logra parsear, None si no
    """
    if not texto:
        return None
    
    # Limpiar espacios y caracteres especiales
    texto = texto.strip()
    
    # Intentar extraer fecha en patrón dd/mm/yyyy
    for formato in FORMATO_FECHAS:
        try:
            return datetime.strptime(texto, formato)
        except ValueError:
            continue
    
    return None

def extraer_fechas_texto(texto: str) -> list[tuple[str, Optional[datetime]]]:
    """
    Extrae fechas de un bloque de texto
    
    Args:
        texto: Texto que puede contener fechas
        
    Returns:
        Lista de tuplas (texto_fecha, datetime)
    """
    # Patrón para buscar posibles fechas (dd/mm/yyyy)
    patron = r'\d{1,2}[/-]\d{1,2}[/-]\d{4}'
    coincidencias = re.findall(patron, texto)
    
    resultados = []
    for match in coincidencias:
        fecha = parsear_fecha(match)
        resultados.append((match, fecha))
    
    return resultados

def identificar_nivel(titulo: str, contenido: str = "") -> str:
    """
    Identifica el nivel (C1, C2, etc.) del texto de la convocatoria
    
    Args:
        titulo: Título de la convocatoria
        contenido: Contenido adicional de la convocatoria
        
    Returns:
        Nivel encontrado o "DESCONOCIDO"
    """
    texto_completo = f"{titulo} {contenido}".upper()
    
    # Buscar C1
    for keyword in KEYWORDS_C1:
        if keyword.upper() in texto_completo:
            return "C1"
    
    # Buscar C2
    for keyword in KEYWORDS_C2:
        if keyword.upper() in texto_completo:
            return "C2"
    
    return "DESCONOCIDO"

def es_nivel_interes(nivel: str) -> bool:
    """
    Verifica si el nivel es de interés (C1 o C2)
    
    Args:
        nivel: Código de nivel
        
    Returns:
        True si es C1 o C2
    """
    return nivel in NIVELES_INTERES

def limpiar_texto(texto: str) -> str:
    """
    Limpia y normaliza texto
    
    Args:
        texto: Texto a limpiar
        
    Returns:
        Texto limpio
    """
    # Eliminar espacios múltiples
    texto = re.sub(r'\s+', ' ', texto)
    # Eliminar espacios al inicio y final
    texto = texto.strip()
    return texto

def formatear_fecha(fecha: Optional[datetime]) -> str:
    """
    Formatea una fecha para visualización
    
    Args:
        fecha: Objeto datetime
        
    Returns:
        Cadena formateada
    """
    if not fecha:
        return "Sin fecha"
    return fecha.strftime("%d/%m/%Y")

def dias_hasta_fecha(fecha: Optional[datetime]) -> Optional[int]:
    """
    Calcula días hasta una fecha
    
    Args:
        fecha: Objeto datetime
        
    Returns:
        Número de días (negativo si ya pasó)
    """
    if not fecha:
        return None
    
    diferencia = fecha - datetime.now()
    return diferencia.days
