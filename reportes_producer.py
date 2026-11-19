"""
Productor de Kafka: simula los reportes que los agentes de tránsito envían
en tiempo real. Toma accidentes reales del dataset de Bucaramanga, les asigna
la hora actual y los publica en el topic 'accidentes_bga' usando la comuna
como llave.

Autora: María Alejandra Herrera
Uso: python3 reportes_producer.py [--csv accidentes.csv] [--por-segundo 4]
"""
import argparse
import csv
import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

TOPIC = "accidentes_bga"
VEHICULOS = ["AUTOMOVIL", "CAMIONETA", "BUS", "CAMION", "MOTO", "BICICLETA"]
COLORES = {"Con Muertos": "\033[1;31m", "Con Heridos": "\033[33m"}


def leer_casos(ruta):
    with open(ruta, encoding="utf-8") as f:
        casos = [fila for fila in csv.DictReader(f) if fila["COMUNA"] != "NO DISPONIBLE"]
    print(f"{len(casos):,} accidentes reales disponibles para la simulación")
    return casos


def a_reporte(fila, numero):
    vehiculos = {v.lower(): int(fila[v] or 0) for v in VEHICULOS}
    return {
        "reporte": numero,
        "hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "comuna": fila["COMUNA"],
        "barrio": fila["BARRIO"],
        "gravedad": fila["GRAVEDAD"].strip().capitalize().replace("muertos", "Muertos").replace("heridos", "Heridos"),
        "peatones": int(fila["PEATON"] or 0),
        **vehiculos,
    }


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--csv", default="accidentes.csv")
    args.add_argument("--por-segundo", type=float, default=4)
    args = args.parse_args()

    casos = leer_casos(args.csv)
    productor = KafkaProducer(bootstrap_servers="localhost:9092",
                              key_serializer=str.encode,
                              value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode())
    numero = 0
    try:
        while True:
            numero += 1
            r = a_reporte(random.choice(casos), numero)
            productor.send(TOPIC, key=r["comuna"], value=r)
            color = COLORES.get(r["gravedad"], "")
            print(f"{color}[{r['hora'][11:]}] #{numero:05d} {r['comuna']:<24} "
                  f"{r['gravedad']:<12} motos={r['moto']} peatones={r['peatones']}\033[0m")
            time.sleep(1 / args.por_segundo)
    except KeyboardInterrupt:
        print(f"\nSimulación detenida: {numero:,} reportes enviados")
    finally:
        productor.flush()
        productor.close()


if __name__ == "__main__":
    main()
