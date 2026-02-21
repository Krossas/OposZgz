from datetime import datetime
from enum import Enum
from dataclasses import dataclass
from typing import Optional

class EstadoConvocatoria(Enum):
    PENDIENTE = "PENDIENTE"
    ABIERTA = "ABIERTA"
    CERRADA = "CERRADA"
    DESCONOCIDO = "DESCONOCIDO"

@dataclass
class Convocatoria:
    titulo: str
    nivel: str
    fecha_inicio: Optional[datetime]
    fecha_fin: Optional[datetime]
    url: str
    url_detalle: str
    fecha_scraping: datetime
    turno: Optional[str] = None
    num_plazas: Optional[int] = None
    estado: EstadoConvocatoria = EstadoConvocatoria.DESCONOCIDO
    
    def __post_init__(self):
        """Calcula automáticamente el estado"""
        self.estado = self.calcular_estado()
    
    def calcular_estado(self) -> EstadoConvocatoria:
        """Determina el estado según fechas"""
        if not self.fecha_inicio or not self.fecha_fin:
            return EstadoConvocatoria.DESCONOCIDO
        
        ahora = datetime.now()
        
        if ahora < self.fecha_inicio:
            return EstadoConvocatoria.PENDIENTE
        elif self.fecha_inicio <= ahora <= self.fecha_fin:
            return EstadoConvocatoria.ABIERTA
        else:
            return EstadoConvocatoria.CERRADA
    
    def __str__(self):
        return (f"{self.titulo} ({self.nivel}) - "
                f"Estado: {self.estado.value} - "
                f"Fin: {self.fecha_fin}")
