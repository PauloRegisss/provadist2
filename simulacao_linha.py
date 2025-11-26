#!/usr/bin/env python3
"""
Simulação síncrona por timesteps de uma linha de produção com:
- buffer limitado (deque)
- semáforos: espaço disponível e itens disponíveis
- mutex (Lock) para proteger o buffer
- produtores e consumidores em threads
- sincronização por Barrier para avanço de timesteps

Modo de uso:
    python simulacao_linha.py

Parâmetros podem ser alterados na função main() ou via argumentos simples.
"""
import threading
from collections import deque
import time
import argparse
import csv

class Stats:
    def __init__(self):
        self.total_produced = 0
        self.total_consumed = 0
        self.prod_blocked_counts = 0  # número de tentativas de produzir que falharam (por timestep)
        self.cons_blocked_counts = 0  # idem para consumidores
        self.buffer_occupancy = []    # snapshot por timestep
        self.lock = threading.Lock()

    def snapshot_occupancy(self, occ):
        with self.lock:
            self.buffer_occupancy.append(occ)

    def inc_produced(self, n=1):
        with self.lock:
            self.total_produced += n

    def inc_consumed(self, n=1):
        with self.lock:
            self.total_consumed += n

    def add_prod_block(self, n=1):
        with self.lock:
            self.prod_blocked_counts += n

    def add_cons_block(self, n=1):
        with self.lock:
            self.cons_blocked_counts += n

class ProductionBuffer:
    def __init__(self, capacity):
        self.buffer = deque()
        self.capacity = capacity
        self.lock = threading.Lock()

    def append(self, item):
        with self.lock:
            self.buffer.append(item)

    def pop(self):
        with self.lock:
            return self.buffer.popleft()

    def occupancy(self):
        with self.lock:
            return len(self.buffer)

def producer_thread(pid, barrier, stop_flag, buffer, space_sem, items_sem, stats, n_timesteps, productivity=1):
    """
    Cada produtor tenta produzir `productivity` itens por timestep (aqui assumimos 1).
    A cada timestep:
      - tenta adquirir espaço (space_sem.acquire(blocking=False))
      - se conseguir, coloca item(s) no buffer, stats.inc_produced, items_sem.release()
      - senão, registra bloqueio e não produz naquele timestep
    """
    timestep = 0
    while not stop_flag.is_set():
        # sincroniza com início do timestep
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            break

        if stop_flag.is_set():
            break

        # a cada timestep, produzir até 'productivity' unidades (aqui 1)
        produced_this_step = 0
        for _ in range(productivity):
            acquired = space_sem.acquire(blocking=False)
            if not acquired:
                stats.add_prod_block(1)
                break
            # produziu: inserir no buffer
            buffer.append((pid, timestep))  # item identifica produtor e timestep (opcional)
            items_sem.release()
            produced_this_step += 1
            stats.inc_produced(1)

        timestep += 1
        if timestep >= n_timesteps:
            # aguardamos a barreira final no main e saímos
            break

    # sair do loop
    return

def consumer_thread(cid, barrier, stop_flag, buffer, space_sem, items_sem, stats, n_timesteps, productivity=1):
    """
    Cada consumidor tenta consumir `productivity` itens por timestep (aqui assumimos 1).
    A cada timestep:
      - tenta adquirir item (items_sem.acquire(blocking=False))
      - se conseguir, remove do buffer, stats.inc_consumed, space_sem.release()
      - senão, registra bloqueio e não consome naquele timestep
    """
    timestep = 0
    while not stop_flag.is_set():
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            break

        if stop_flag.is_set():
            break

        consumed_this_step = 0
        for _ in range(productivity):
            acquired = items_sem.acquire(blocking=False)
            if not acquired:
                stats.add_cons_block(1)
                break
            # remove do buffer
            try:
                item = buffer.pop()
            except IndexError:
                # buffer inconsistente — só para segurança
                # liberar de volta e registrar bloqueio
                space_sem.release()
                stats.add_cons_block(1)
                break
            # liberar espaço
            space_sem.release()
            consumed_this_step += 1
            stats.inc_consumed(1)

        timestep += 1
        if timestep >= n_timesteps:
            break

    return

def run_simulation(buffer_capacity, n_producers, n_consumers, n_timesteps, timestep_duration=0.0, productivity=1, save_csv=None):
    # inicializa estruturas
    buffer = ProductionBuffer(buffer_capacity)
    space_sem = threading.Semaphore(buffer_capacity)
    items_sem = threading.Semaphore(0)
    stats = Stats()
    stop_flag = threading.Event()

    # barrier: produtores + consumidores + controlador (main)
    parties = n_producers + n_consumers + 1
    barrier = threading.Barrier(parties)

    # criar threads
    producers = []
    consumers = []

    for i in range(n_producers):
        t = threading.Thread(target=producer_thread, args=(f"P{i+1}", barrier, stop_flag, buffer, space_sem, items_sem, stats, n_timesteps, productivity), daemon=True)
        producers.append(t)

    for i in range(n_consumers):
        t = threading.Thread(target=consumer_thread, args=(f"C{i+1}", barrier, stop_flag, buffer, space_sem, items_sem, stats, n_timesteps, productivity), daemon=True)
        consumers.append(t)

    # iniciar threads
    for t in producers + consumers:
        t.start()

    # loop principal por timesteps: a cada timestep, liberamos a barrier para que threads façam uma tentativa.
    for ts in range(n_timesteps):
        # espera até que todos participantes (threads) cheguem na barrier e prossigam
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            break

        # depois que todos fizeram suas tentativas no timestep ts, coletamos snapshot do buffer
        occ = buffer.occupancy()
        stats.snapshot_occupancy(occ)

        # opcional: sleep para simular duração do timestep (em segundos)
        if timestep_duration > 0:
            time.sleep(timestep_duration)

    # finaliza
    stop_flag.set()
    # quebrar barrier para liberar threads bloqueadas
    try:
        barrier.abort()
    except Exception:
        pass

    # aguardar threads terminarem
    for t in producers + consumers:
        t.join(timeout=1.0)

    # montar relatório rápido
    report = {
        "buffer_capacity": buffer_capacity,
        "n_producers": n_producers,
        "n_consumers": n_consumers,
        "n_timesteps": n_timesteps,
        "total_produced": stats.total_produced,
        "total_consumed": stats.total_consumed,
        "prod_blocked_counts": stats.prod_blocked_counts,
        "cons_blocked_counts": stats.cons_blocked_counts,
        "final_buffer_occupancy": buffer.occupancy(),
    }

    # salvar CSV de ocupação por timestep (opcional)
    if save_csv:
        with open(save_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestep", "occupancy"])
            for i, occ in enumerate(stats.buffer_occupancy):
                writer.writerow([i, occ])

    return report, stats

def parse_args():
    p = argparse.ArgumentParser(description="Simulação de linha de produção multithread por timesteps")
    p.add_argument("--buffer", type=int, default=10, help="capacidade do buffer (min 1)")
    p.add_argument("--producers", type=int, default=2, help="nº produtores")
    p.add_argument("--consumers", type=int, default=3, help="nº consumidores")
    p.add_argument("--timesteps", type=int, default=100, help="nº timesteps")
    p.add_argument("--timestep-duration", type=float, default=0.0, help="duração do timestep (s) para visualização")
    p.add_argument("--save-csv", type=str, default=None, help="caminho para salvar ocupação por timestep (.csv)")
    return p.parse_args()

def main():
    args = parse_args()
    print("Iniciando simulação com parâmetros:")
    print(f" buffer capacity: {args.buffer}")
    print(f" producers: {args.producers}")
    print(f" consumers: {args.consumers}")
    print(f" timesteps: {args.timesteps}")
    print(f" timestep duration (s): {args.timestep_duration}")

    report, stats = run_simulation(
        buffer_capacity=args.buffer,
        n_producers=args.producers,
        n_consumers=args.consumers,
        n_timesteps=args.timesteps,
        timestep_duration=args.timestep_duration,
        productivity=1,
        save_csv=args.save_csv
    )

    print("\n--- Relatório resumido ---")
    for k, v in report.items():
        print(f"{k}: {v}")
    print("\nExemplo de métricas adicionais:")
    print(f" produtor(es) bloqueados (total tentativas falhas): {stats.prod_blocked_counts}")
    print(f" consumidor(es) bloqueados (total tentativas falhas): {stats.cons_blocked_counts}")

    if args.save_csv:
        print(f"Ocupação por timestep salva em: {args.save_csv}")

if __name__ == "__main__":
    main()
