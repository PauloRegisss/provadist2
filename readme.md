# Simulação de Linha de Produção Concorrente (Produtor-Consumidor)

Projeto desenvolvido para a disciplina de Sistemas Distribuídos. Implementa uma simulação de linha de produção industrial utilizando **Python**, **Multithreading**, **Semáforos** e **Mutexes**.

## 📋 Descrição do Projeto
O objetivo é simular o problema clássico do "Produtor-Consumidor" em um cenário industrial onde:
1. **Produtores** geram itens com tempo de processamento variável.
2. **Consumidores** retiram itens para processamento posterior.
3. Um **Buffer Limitado** (Fila) intermedeia as operações.

O sistema garante a integridade dos dados e evita *Race Conditions* e *Deadlocks* através de primitivas de sincronização.

## 🚀 Funcionalidades
- **Sincronização:** Uso de `threading.Semaphore` (para controle de cheios/vazios) e `threading.Lock` (para exclusão mútua no buffer).
- **Simulação Realista:** Introdução de atrasos aleatórios (`random.uniform`) para simular a complexidade variável de processamento.
- **Otimização de Performance:** Implementação de processamento de tempo em lote para evitar *Thread Thrashing* em simulações longas (1.000.000 timesteps).
- **Analytics:** Geração automática de relatórios CSV e gráficos de desempenho (Eficiência, Gargalos e Ocupação de Buffer).

## 🛠️ Requisitos
* Python 3.8+
* Bibliotecas listadas em `requirements.txt`
