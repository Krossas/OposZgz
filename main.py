#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Gestor de Oposiciones C1/C2 - Ayuntamiento de Zaragoza
Interfaz Streamlit
"""

import subprocess
import sys

if __name__ == "__main__":
    # Lanzar Streamlit
    subprocess.run([sys.executable, "-m", "streamlit", "run", "app.py"])
