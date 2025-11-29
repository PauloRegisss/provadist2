import threading
import time
import queue
import os
import random
from dataclasses import dataclass, field
from typing import List
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

os.makedirs("results", exist_ok=True)
os.makedirs("charts", exist_ok=True)
os.makedirs("reports", exist_ok=True)

@dataclass
class ProductionMetrics:
    total_produced: int = 0
    total_consumed: int = 0
    producer_wait_times: List[float] = field(default_factory=list)
    consumer_wait_times: List[float] = field(default_factory=list)
    buffer_history: List[int] = field(default_factory=list)

class ProductionLine:
    def __init__(self, buffer_capacity: int, num_producers: int, 
                 num_consumers: int, total_timesteps: int, check_constraints: bool = True):
        
        if check_constraints:
            if buffer_capacity < 1000: print(f"⚠️ Aviso: Buffer {buffer_capacity} < 1000 (Requisito).")
            if num_producers < 200: print(f"⚠️ Aviso: Produtores {num_producers} < 200 (Requisito).")
            if total_timesteps < 1000000: print(f"⚠️ Aviso: Timesteps {total_timesteps} < 1M (Requisito).")

        self.buffer_capacity = buffer_capacity
        self.num_producers = num_producers
        self.num_consumers = num_consumers
        self.total_timesteps = total_timesteps
        
        self.buffer = queue.Queue(maxsize=buffer_capacity)
        
        self.empty_slots = threading.Semaphore(buffer_capacity)
        self.filled_slots = threading.Semaphore(0)
        self.mutex = threading.Lock()
        
        self.current_timestep = 0
        self.timestep_lock = threading.Lock()
        self.running = True
        
        self.metrics = ProductionMetrics()
        
        self.producer_threads = []
        self.consumer_threads = []
        self.start_time = None

    def producer(self, producer_id: int):
        """Produtor com simulação de trabalho"""
        while self.running:
            if self.current_timestep >= self.total_timesteps:
                break

            time.sleep(random.uniform(0.0001, 0.0002))

            wait_start = time.time()
            
            if not self.empty_slots.acquire(timeout=0.05):
                continue 

            wait_time = time.time() - wait_start

            with self.mutex:
                if self.current_timestep <= self.total_timesteps:
                    item = f"P{producer_id}_T{self.current_timestep}"
                    self.buffer.put(item) 
                    
                    self.metrics.total_produced += 1
                    self.metrics.producer_wait_times.append(wait_time)
                    
                    threshold = 1.0 if self.total_timesteps < 1000 else 0.005
                    if random.random() < threshold: 
                        self.metrics.buffer_history.append(self.buffer.qsize())
            
            self.filled_slots.release()

    def consumer(self, consumer_id: int):
        """Consumidor com simulação de trabalho"""
        while self.running:
            if self.current_timestep >= self.total_timesteps and self.buffer.empty():
                break

            wait_start = time.time()
            
            if not self.filled_slots.acquire(timeout=0.05):
                continue 

            wait_time = time.time() - wait_start

            with self.mutex:
                if not self.buffer.empty():
                    self.buffer.get()
                    self.metrics.total_consumed += 1
                    self.metrics.consumer_wait_times.append(wait_time)
            
            self.empty_slots.release()
            
            time.sleep(random.uniform(0.0001, 0.0002))

    def timestep_controller(self):
        """
        Controlador Híbrido: Preciso no Toy Problem, Rápido no Experimento Final.
        """
        print(f"Iniciando {self.total_timesteps} timesteps...")
        print_interval = max(1, self.total_timesteps // 10)
        
        batch_size = 1 if self.total_timesteps <= 1000 else 500
        sleep_time = 0.05 if self.total_timesteps <= 1000 else 0.001

        while self.current_timestep < self.total_timesteps:
            with self.timestep_lock:
                self.current_timestep += 1
            
            if self.current_timestep % batch_size == 0:
                time.sleep(sleep_time) 
            
            if self.current_timestep % print_interval == 0:
                print(f"   Progresso: {self.current_timestep}/{self.total_timesteps} ({int(self.current_timestep/self.total_timesteps*100)}%)")
        
        self.running = False
        print("Tempo esgotado. Finalizando threads...")

    def run(self):
        self.start_time = time.time()
        print(f"-> Setup: Buffer={self.buffer_capacity}, Prod={self.num_producers}, Cons={self.num_consumers}")

        controller = threading.Thread(target=self.timestep_controller, daemon=True)
        controller.start()

        # Inicia Workers
        for i in range(self.num_producers):
            t = threading.Thread(target=self.producer, args=(i,), daemon=True)
            self.producer_threads.append(t)
            t.start()
            
        for i in range(self.num_consumers):
            t = threading.Thread(target=self.consumer, args=(i,), daemon=True)
            self.consumer_threads.append(t)
            t.start()

        controller.join()
        
        for t in self.producer_threads: t.join(timeout=0.1)
        for t in self.consumer_threads: t.join(timeout=0.1)
            
        return self.metrics, time.time() - self.start_time


def run_experiment(config):
    sim = ProductionLine(
        buffer_capacity=config['buffer_capacity'],
        num_producers=config['num_producers'],
        num_consumers=config['num_consumers'],
        total_timesteps=config['total_timesteps'],
        check_constraints=config.get('check_constraints', True)
    )
    metrics, exec_time = sim.run()
    
    avg_prod_wait = sum(metrics.producer_wait_times) / len(metrics.producer_wait_times) if metrics.producer_wait_times else 0
    avg_cons_wait = sum(metrics.consumer_wait_times) / len(metrics.consumer_wait_times) if metrics.consumer_wait_times else 0
    efficiency = (metrics.total_consumed / metrics.total_produced * 100) if metrics.total_produced > 0 else 0
    
    return {
        **config,
        'total_produced': metrics.total_produced,
        'total_consumed': metrics.total_consumed,
        'execution_time': round(exec_time, 2),
        'avg_producer_wait': avg_prod_wait,
        'avg_consumer_wait': avg_cons_wait,
        'efficiency': efficiency,
        'buffer_history': metrics.buffer_history
    }

def generate_charts(results, timestamp):
    csv_data = [{k:v for k,v in r.items() if k != 'buffer_history'} for r in results]
    df = pd.DataFrame(csv_data)
    
    if len(results) > 1:
        plt.figure(figsize=(8, 5))
        plt.bar(df.index.astype(str), df['efficiency'], color='skyblue', edgecolor='black')
        plt.xlabel('Experimento')
        plt.ylabel('Eficiência (%)')
        plt.title('Eficiência da Linha de Produção')
        plt.savefig(f'charts/efficiency_{timestamp}.png')
        plt.close()

        plt.figure(figsize=(8, 5))
        plt.plot(df.index.astype(str), df['avg_producer_wait'], 'o-', label='Produtores')
        plt.plot(df.index.astype(str), df['avg_consumer_wait'], 'x-', label='Consumidores')
        plt.title('Tempo Médio de Espera (Gargalos)')
        plt.ylabel('Segundos')
        plt.legend()
        plt.savefig(f'charts/wait_times_{timestamp}.png')
        plt.close()

    for i, res in enumerate(results):
        if res['buffer_history']:
            plt.figure(figsize=(10, 4))
            plt.plot(res['buffer_history'], linewidth=1) 
            plt.axhline(y=res['buffer_capacity'], color='r', linestyle='--', label='Max')
            plt.title(f"Ocupação do Buffer - {('Toy Problem' if res['total_timesteps'] < 1000 else f'Exp {i}')}")
            plt.xlabel("Amostras")
            plt.ylabel("Itens no Buffer")
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.savefig(f'charts/buffer_exp_{i}_{timestamp}.png')
            plt.close()

def main():
    print("="*60)
    print("PROJETO DISTRIBUÍDO N2 - SIMULAÇÃO DE PRODUÇÃO")
    print("="*60)
    
    print("1. Rodar Toy Problem (Buffer=10, 2 Prod, 3 Cons, 100 Steps)")
    print("2. Rodar Experimentos Completos (Requisitos Finais 1M Steps)")
    choice = input("Escolha (1 ou 2): ").strip()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = []

    if choice == '1':
        config = {
            'buffer_capacity': 10,
            'num_producers': 2,
            'num_consumers': 3,
            'total_timesteps': 100,
            'check_constraints': False
        }
        print("\n--- Executando TOY PROBLEM ---")
        results.append(run_experiment(config))

    elif choice == '2':
        TIMESTEPS = 1000000 
        configs = [
            {'buffer_capacity': 1000, 'num_producers': 200, 'num_consumers': 200, 'total_timesteps': TIMESTEPS},
            {'buffer_capacity': 5000, 'num_producers': 200, 'num_consumers': 200, 'total_timesteps': TIMESTEPS},
            {'buffer_capacity': 1000, 'num_producers': 200, 'num_consumers': 250, 'total_timesteps': TIMESTEPS},
            {'buffer_capacity': 1000, 'num_producers': 300, 'num_consumers': 200, 'total_timesteps': TIMESTEPS},
        ]
        
        print(f"\n--- Executando {len(configs)} Experimentos Completos ---")
        for i, conf in enumerate(configs):
            print(f"\n--- Experimento {i} ---")
            res = run_experiment(conf)
            results.append(res)
            print(f"Eficiência: {res['efficiency']:.2f}%")

    if results:
        csv_data = [{k:v for k,v in r.items() if k != 'buffer_history'} for r in results]
        path = f'results/report_{timestamp}.csv'
        pd.DataFrame(csv_data).to_csv(path, index=False)
        print(f"\nResultados salvos em: {path}")
        
        print("\n" + "="*80)
        print(pd.DataFrame(csv_data)[['buffer_capacity', 'num_producers', 'num_consumers', 'total_produced', 'efficiency']].to_string())
        print("="*80)

        generate_charts(results, timestamp)
        print(f"Gráficos gerados na pasta 'charts/'.")

if __name__ == "__main__":
    main()