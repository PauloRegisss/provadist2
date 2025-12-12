import multiprocessing as mp
from multiprocessing import Process, Queue, Manager, Event
import time
import random
from dataclasses import dataclass, field, asdict
from typing import List, Literal, Dict
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
import json

# Criar pastas para outputs
os.makedirs('logs', exist_ok=True)
os.makedirs('graficos', exist_ok=True)
os.makedirs('relatorios', exist_ok=True)

# --- CONFIGURAÇÕES ---

# CENÁRIOS PARA TESTE (requisito: variar parâmetros)
CENARIOS = {
    'Cenario_1_Estavel': {
        'TIMESTEPS': 1000,
        'MIN_REQ_PER_TICK': 10,
        'MAX_REQ_PER_TICK': 50,
        'BUFFER_SIZE': 5000,
        'PROB_FALHA_ATENDENTE': 0.001,
        'PROB_FALHA_SERVIDOR': 0.01,
        'TEMPO_RECOVERY_SERVIDOR': 20,
        'MIN_ATENDENTES_POR_TIPO': 100,
        'FALHAS_PROGRAMADAS': [200, 500, 800],
        'DELAY_TICK': 0.01
    },
    'Cenario_2_Carga_Alta': {
        'TIMESTEPS': 1000,
        'MIN_REQ_PER_TICK': 50,
        'MAX_REQ_PER_TICK': 200,
        'BUFFER_SIZE': 10000,
        'PROB_FALHA_ATENDENTE': 0.001,
        'PROB_FALHA_SERVIDOR': 0.02,
        'TEMPO_RECOVERY_SERVIDOR': 30,
        'MIN_ATENDENTES_POR_TIPO': 100,
        'FALHAS_PROGRAMADAS': [300, 600, 900],
        'DELAY_TICK': 0.01
    },
    'Cenario_3_Alta_Instabilidade': {
        'TIMESTEPS': 1000,
        'MIN_REQ_PER_TICK': 20,
        'MAX_REQ_PER_TICK': 100,
        'BUFFER_SIZE': 5000,
        'PROB_FALHA_ATENDENTE': 0.005,  # 5x mais falhas
        'PROB_FALHA_SERVIDOR': 0.05,    # 5x mais falhas
        'TEMPO_RECOVERY_SERVIDOR': 50,   # Recuperação mais lenta
        'MIN_ATENDENTES_POR_TIPO': 100,
        'FALHAS_PROGRAMADAS': [100, 200, 300, 400, 500, 600, 700, 800],
        'DELAY_TICK': 0.01
    }
}

# Cenário padrão para execução única
CONFIG = CENARIOS['Cenario_1_Estavel']

# --- CLASSES DE DADOS ---

@dataclass
class Atendente:
    """Atendente individual (Suporte ou Vendas)"""
    id: str
    tipo: Literal['suporte', 'vendas']
    servidor: str
    ativo: bool = True
    atendimentos: int = 0
    tick_falha: int = None
    
    def to_dict(self):
        return asdict(self)


@dataclass
class Requisicao:
    """Solicitação de cliente"""
    id: int
    tipo: Literal['suporte', 'vendas']
    criada_em: int
    servidor_atual: str = None
    atendente_atual: str = None
    transferencias: List[Dict] = field(default_factory=list)
    finalizada: bool = False
    tick_finalizacao: int = None
    
    def transferir(self, origem, destino, motivo, tick):
        self.transferencias.append({
            'tick': tick,
            'origem': origem,
            'destino': destino,
            'motivo': motivo
        })
        self.servidor_atual = destino
    
    def to_dict(self):
        return {
            'id': self.id,
            'tipo': self.tipo,
            'criada_em': self.criada_em,
            'servidor_atual': self.servidor_atual,
            'atendente_atual': self.atendente_atual,
            'transferencias': self.transferencias,
            'finalizada': self.finalizada,
            'tick_finalizacao': self.tick_finalizacao
        }
    
    @staticmethod
    def from_dict(data):
        req = Requisicao(
            id=data['id'],
            tipo=data['tipo'],
            criada_em=data['criada_em']
        )
        req.servidor_atual = data.get('servidor_atual')
        req.atendente_atual = data.get('atendente_atual')
        req.transferencias = data.get('transferencias', [])
        req.finalizada = data.get('finalizada', False)
        req.tick_finalizacao = data.get('tick_finalizacao')
        return req


# --- SERVIDOR DISTRIBUÍDO ---

class ServidorDistribuido(Process):
    """
    Servidor que roda em PROCESSO INDEPENDENTE
    Contém atendentes individuais que processam requisições
    """
    
    def __init__(self, nome, capacidade_atendentes, config,
                 fila_entrada, fila_logs, fila_failover_out, 
                 fila_failover_in, evento_shutdown, estado_compartilhado):
        super().__init__()
        self.nome = nome
        self.capacidade_atendentes = capacidade_atendentes
        self.config = config
        self.fila_entrada = fila_entrada
        self.fila_logs = fila_logs
        self.fila_failover_out = fila_failover_out
        self.fila_failover_in = fila_failover_in
        self.evento_shutdown = evento_shutdown
        self.estado_compartilhado = estado_compartilhado
        
        self.atendentes = []
        self.fila_local_suporte = []
        self.fila_local_vendas = []
        self.pool_reservas = []
        
        self.ativo = True
        self.tick_atual = 0
        self.downtime_counter = 0
        self.total_atendimentos = 0
        self.falhas_servidor = 0
        
        self._criar_atendentes()
        self._criar_pool_reservas()
    
    def _criar_atendentes(self):
        """Cria atendentes respeitando mínimo de 100 de cada tipo"""
        min_por_tipo = self.config['MIN_ATENDENTES_POR_TIPO']
        
        # Garante mínimo de suporte
        for i in range(min_por_tipo):
            self.atendentes.append(Atendente(
                id=f"{self.nome}-SUP{i:04d}",
                tipo='suporte',
                servidor=self.nome
            ))
        
        # Garante mínimo de vendas
        for i in range(min_por_tipo):
            self.atendentes.append(Atendente(
                id=f"{self.nome}-VEN{i:04d}",
                tipo='vendas',
                servidor=self.nome
            ))
        
        # Preenche restante aleatoriamente
        restante = self.capacidade_atendentes - (min_por_tipo * 2)
        for i in range(max(0, restante)):
            tipo = random.choice(['suporte', 'vendas'])
            prefixo = 'SUP' if tipo == 'suporte' else 'VEN'
            self.atendentes.append(Atendente(
                id=f"{self.nome}-{prefixo}{min_por_tipo + i:04d}",
                tipo=tipo,
                servidor=self.nome
            ))
        
        self._log('INFO', f"Criados {len(self.atendentes)} atendentes " +
                  f"(Suporte: {sum(1 for a in self.atendentes if a.tipo == 'suporte')}, " +
                  f"Vendas: {sum(1 for a in self.atendentes if a.tipo == 'vendas')})")
    
    def _criar_pool_reservas(self):
        """Pool de atendentes de reserva para substituições"""
        for i in range(1000):
            tipo = 'suporte' if i % 2 == 0 else 'vendas'
            prefixo = 'SUP' if tipo == 'suporte' else 'VEN'
            self.pool_reservas.append(Atendente(
                id=f"{self.nome}-RESERVA-{prefixo}{i:04d}",
                tipo=tipo,
                servidor=self.nome
            ))
    
    def run(self):
        """Loop principal do processo"""
        print(f"[{self.nome}] Processo iniciado (PID: {mp.current_process().pid})")
        self._log('INICIO', f"Servidor iniciado com PID {mp.current_process().pid}")
        
        # Debug: Mostrar atendentes criados
        suporte_count = sum(1 for a in self.atendentes if a.tipo == 'suporte')
        vendas_count = sum(1 for a in self.atendentes if a.tipo == 'vendas')
        print(f"[{self.nome}] {len(self.atendentes)} atendentes: {suporte_count} suporte, {vendas_count} vendas")
        
        while not self.evento_shutdown.is_set() and self.tick_atual < self.config['TIMESTEPS']:
            self.tick_atual += 1
            
            try:
                # 1. Verificar falha do servidor
                self._verificar_falha_servidor()
                
                if not self.ativo:
                    self._tentar_recuperacao()
                    self._atualizar_estado()
                    time.sleep(0.001)
                    continue
                
                # 2. Receber requisições de entrada
                self._processar_fila_entrada()
                
                # 3. Receber failover
                self._processar_failover_entrada()
                
                # 4. Verificar falhas de atendentes
                self._verificar_falhas_atendentes()
                
                # 5. Processar requisições com atendentes
                atendidos = self._processar_requisicoes()
                self.total_atendimentos += atendidos
                
                # Debug periódico
                if self.tick_atual % 100 == 0:
                    print(f"[{self.nome}] Tick {self.tick_atual}: " +
                          f"Filas: S={len(self.fila_local_suporte)} V={len(self.fila_local_vendas)} | " +
                          f"Atendidos: {self.total_atendimentos}")
                
                # 6. Verificar buffer overflow
                if not self._verificar_buffer():
                    self._log('CRITICO', f"BUFFER OVERFLOW no tick {self.tick_atual}")
                    self.evento_shutdown.set()
                    break
                
                # 7. Atualizar estado compartilhado
                self._atualizar_estado()
                
                # Delay configurável entre ticks
                time.sleep(self.config.get('DELAY_TICK', 0.001))
                
            except Exception as e:
                self._log('ERRO', f"Exceção no tick {self.tick_atual}: {e}")
                import traceback
                traceback.print_exc()
        
        self._log('FIM', f"Processo finalizado. Total: {self.total_atendimentos} atendimentos")
        print(f"[{self.nome}] Finalizado. Total: {self.total_atendimentos}")
    
    def _verificar_falha_servidor(self):
        """Verifica falhas programadas e aleatórias do servidor"""
        if not self.ativo:
            return
        
        # Falha programada
        if self.tick_atual in self.config['FALHAS_PROGRAMADAS']:
            self.ativo = False
            self.downtime_counter = self.config['TEMPO_RECOVERY_SERVIDOR']
            self.falhas_servidor += 1
            self._log('FALHA_SERVIDOR', f"FALHA PROGRAMADA no tick {self.tick_atual}")
            self._executar_failover()
            return
        
        # Falha aleatória
        if random.random() < self.config['PROB_FALHA_SERVIDOR']:
            self.ativo = False
            self.downtime_counter = self.config['TEMPO_RECOVERY_SERVIDOR']
            self.falhas_servidor += 1
            self._log('FALHA_SERVIDOR', f"FALHA ALEATÓRIA no tick {self.tick_atual}")
            self._executar_failover()
    
    def _tentar_recuperacao(self):
        """Tenta recuperar servidor após falha"""
        self.downtime_counter -= 1
        if self.downtime_counter <= 0:
            self.ativo = True
            self._log('RECUPERACAO', f"Servidor RECUPERADO no tick {self.tick_atual}")
    
    def _executar_failover(self):
        """Transfere todas as requisições para outros servidores"""
        total = len(self.fila_local_suporte) + len(self.fila_local_vendas)
        self._log('FAILOVER', f"Iniciando failover de {total} requisições")
        
        for req in self.fila_local_suporte + self.fila_local_vendas:
            req.transferir(self.nome, 'FAILOVER', 'FALHA_SERVIDOR', self.tick_atual)
            self.fila_failover_out.put(req.to_dict())
            
            # Notificar cliente
            self._log('NOTIFICACAO_CLIENTE', 
                     f"Req {req.id} transferida devido a falha do servidor")
        
        self.fila_local_suporte.clear()
        self.fila_local_vendas.clear()
    
    def _processar_fila_entrada(self):
        """Recebe requisições do load balancer"""
        processadas = 0
        while not self.fila_entrada.empty() and processadas < 1000:
            try:
                msg = self.fila_entrada.get_nowait()
                req = Requisicao.from_dict(msg)
                req.servidor_atual = self.nome
                
                if req.tipo == 'suporte':
                    self.fila_local_suporte.append(req)
                else:
                    self.fila_local_vendas.append(req)
                
                processadas += 1
                self._log('RECEBIMENTO', f"Req {req.id} ({req.tipo}) recebida na fila")
                
            except Exception as e:
                break
    
    def _processar_failover_entrada(self):
        """Recebe requisições de failover"""
        processadas = 0
        while not self.fila_failover_in.empty() and processadas < 1000:
            try:
                msg = self.fila_failover_in.get_nowait()
                req = Requisicao.from_dict(msg)
                req.servidor_atual = self.nome
                
                if req.tipo == 'suporte':
                    self.fila_local_suporte.append(req)
                else:
                    self.fila_local_vendas.append(req)
                
                self._log('FAILOVER_RECEBIDO', f"Req {req.id} recebida via failover")
                processadas += 1
                
            except:
                break
    
    def _verificar_falhas_atendentes(self):
        """Verifica e substitui atendentes que falharam"""
        for atendente in list(self.atendentes):
            if not atendente.ativo:
                continue
            
            if random.random() < self.config['PROB_FALHA_ATENDENTE']:
                atendente.ativo = False
                atendente.tick_falha = self.tick_atual
                
                self._log('FALHA_ATENDENTE', 
                         f"Atendente {atendente.id} ({atendente.tipo}) FALHOU")
                
                # Substituir por reserva
                if self.pool_reservas:
                    novo = self.pool_reservas.pop(0)
                    novo.tipo = atendente.tipo
                    novo.servidor = self.nome
                    novo.ativo = True
                    self.atendentes.append(novo)
                    
                    self._log('ENTRADA_ATENDENTE', 
                             f"Atendente {novo.id} ENTROU substituindo {atendente.id}")
    
    def _processar_requisicoes(self):
        """Processa requisições - 1 por atendente ativo por tick"""
        atendidos = 0
        
        # Processar suporte
        atendentes_suporte = [a for a in self.atendentes 
                              if a.ativo and a.tipo == 'suporte']
        
        for atendente in atendentes_suporte:
            if not self.fila_local_suporte:
                break
            
            req = self.fila_local_suporte.pop(0)
            req.finalizada = True
            req.atendente_atual = atendente.id
            req.tick_finalizacao = self.tick_atual
            
            atendente.atendimentos += 1
            atendidos += 1
            
            self._log('ATENDIMENTO', 
                     f"Req {req.id} FINALIZADA por {atendente.id} " +
                     f"({len(req.transferencias)} transferências)")
            
            # Notificar cliente sobre conclusão
            if req.transferencias:
                self._log('NOTIFICACAO_CLIENTE',
                         f"Req {req.id} finalizada após {len(req.transferencias)} " +
                         f"transferência(s) por {atendente.id}")
        
        # Processar vendas
        atendentes_vendas = [a for a in self.atendentes 
                             if a.ativo and a.tipo == 'vendas']
        
        for atendente in atendentes_vendas:
            if not self.fila_local_vendas:
                break
            
            req = self.fila_local_vendas.pop(0)
            req.finalizada = True
            req.atendente_atual = atendente.id
            req.tick_finalizacao = self.tick_atual
            
            atendente.atendimentos += 1
            atendidos += 1
            
            self._log('ATENDIMENTO',
                     f"Req {req.id} FINALIZADA por {atendente.id} " +
                     f"({len(req.transferencias)} transferências)")
            
            if req.transferencias:
                self._log('NOTIFICACAO_CLIENTE',
                         f"Req {req.id} finalizada após {len(req.transferencias)} " +
                         f"transferência(s) por {atendente.id}")
        
        return atendidos
    
    def _verificar_buffer(self):
        """Verifica se buffer não estourou"""
        total = (len(self.fila_local_suporte) + 
                len(self.fila_local_vendas))
        
        if total > self.config['BUFFER_SIZE']:
            return False
        return True
    
    def _atualizar_estado(self):
        """Atualiza estado compartilhado para monitoramento"""
        self.estado_compartilhado[self.nome] = {
            'ativo': self.ativo,
            'tick': self.tick_atual,
            'fila_suporte': len(self.fila_local_suporte),
            'fila_vendas': len(self.fila_local_vendas),
            'total_fila': len(self.fila_local_suporte) + len(self.fila_local_vendas),
            'atendentes_suporte_ativos': sum(1 for a in self.atendentes 
                                              if a.ativo and a.tipo == 'suporte'),
            'atendentes_vendas_ativos': sum(1 for a in self.atendentes 
                                             if a.ativo and a.tipo == 'vendas'),
            'total_atendimentos': self.total_atendimentos,
            'falhas_servidor': self.falhas_servidor,
            'pid': mp.current_process().pid
        }
    
    def _log(self, tipo, mensagem):
        """Envia log para fila central"""
        try:
            self.fila_logs.put({
                'tick': self.tick_atual,
                'servidor': self.nome,
                'tipo': tipo,
                'mensagem': mensagem,
                'timestamp': time.time()
            })
        except:
            pass


# --- LOAD BALANCER ---

class LoadBalancer(Process):
    """Distribui requisições entre servidores"""
    
    def __init__(self, filas_servidores, fila_logs, evento_shutdown, 
                 estado_compartilhado, config):
        super().__init__()
        self.filas_servidores = filas_servidores
        self.fila_logs = fila_logs
        self.evento_shutdown = evento_shutdown
        self.estado_compartilhado = estado_compartilhado
        self.config = config
        self.req_counter = 0
        self.tick_atual = 0
    
    def run(self):
        print(f"[LoadBalancer] Iniciado (PID: {mp.current_process().pid})")
        
        while not self.evento_shutdown.is_set() and self.tick_atual < self.config['TIMESTEPS']:
            self.tick_atual += 1
            
            # Gerar requisições
            qtd = random.randint(self.config['MIN_REQ_PER_TICK'], 
                                 self.config['MAX_REQ_PER_TICK'])
            
            requisicoes_enviadas = 0
            for _ in range(qtd):
                req = Requisicao(
                    id=self.req_counter,
                    tipo=random.choice(['suporte', 'vendas']),
                    criada_em=self.tick_atual
                )
                self.req_counter += 1
                
                # Escolher servidor
                servidor = self._escolher_servidor(req.tipo)
                
                if servidor:
                    try:
                        self.filas_servidores[servidor].put(req.to_dict(), block=False)
                        requisicoes_enviadas += 1
                    except Exception as e:
                        pass
            
            # Log periódico para debug
            if self.tick_atual % 100 == 0:
                self._log('INFO', f"Tick {self.tick_atual}: {requisicoes_enviadas}/{qtd} requisições enviadas")
            
            # Delay configurável
            time.sleep(self.config.get('DELAY_TICK', 0.001))
        
        print(f"[LoadBalancer] Finalizado. {self.req_counter} requisições geradas")
    
    def _log(self, tipo, mensagem):
        """Envia log para fila central"""
        try:
            self.fila_logs.put({
                'tick': self.tick_atual,
                'servidor': 'LoadBalancer',
                'tipo': tipo,
                'mensagem': mensagem,
                'timestamp': time.time()
            })
        except:
            pass
    
    def _escolher_servidor(self, tipo):
        """Escolhe servidor com menor fila do tipo apropriado"""
        candidatos = []
        
        for nome, estado in self.estado_compartilhado.items():
            if estado.get('ativo', False):
                fila = estado.get(f'fila_{tipo}', 9999)
                candidatos.append((nome, fila))
        
        if not candidatos:
            return None
        
        return min(candidatos, key=lambda x: x[1])[0]


# --- COORDENADOR DE FAILOVER ---

class FailoverCoordinator(Process):
    """Redistribui requisições quando servidores falham"""
    
    def __init__(self, fila_failover, filas_servidores, fila_logs,
                 evento_shutdown, estado_compartilhado, config):
        super().__init__()
        self.fila_failover = fila_failover
        self.filas_servidores = filas_servidores
        self.fila_logs = fila_logs
        self.evento_shutdown = evento_shutdown
        self.estado_compartilhado = estado_compartilhado
        self.config = config
    
    def run(self):
        print(f"[FailoverCoord] Iniciado (PID: {mp.current_process().pid})")
        
        while not self.evento_shutdown.is_set():
            try:
                while not self.fila_failover.empty():
                    req_dict = self.fila_failover.get_nowait()
                    req = Requisicao.from_dict(req_dict)
                    
                    # Encontrar servidor backup
                    servidor_backup = self._escolher_backup(req.tipo)
                    
                    if servidor_backup:
                        self.filas_servidores[servidor_backup]['failover'].put(req.to_dict())
                
                time.sleep(0.001)
            except:
                pass
        
        print("[FailoverCoord] Finalizado")
    
    def _escolher_backup(self, tipo):
        """Escolhe servidor com menor carga do tipo"""
        candidatos = []
        
        for nome, estado in self.estado_compartilhado.items():
            if estado.get('ativo', False):
                carga = estado.get(f'fila_{tipo}', 9999)
                candidatos.append((nome, carga))
        
        if not candidatos:
            return None
        
        return min(candidatos, key=lambda x: x[1])[0]


# --- MONITOR DE LOGS ---

class MonitorLogs(Process):
    """Coleta logs e salva em arquivo"""
    
    def __init__(self, fila_logs, evento_shutdown):
        super().__init__()
        self.fila_logs = fila_logs
        self.evento_shutdown = evento_shutdown
        self.logs = []
    
    def run(self):
        print(f"[Monitor] Iniciado (PID: {mp.current_process().pid})")
        
        while not self.evento_shutdown.is_set():
            try:
                while not self.fila_logs.empty():
                    log = self.fila_logs.get_nowait()
                    self.logs.append(log)
                
                time.sleep(0.01)
            except:
                pass
        
        # Processar logs restantes na fila após shutdown
        print("[Monitor] Processando logs finais...")
        tentativas = 0
        while tentativas < 100:  # Tentar por 1 segundo
            try:
                while not self.fila_logs.empty():
                    log = self.fila_logs.get_nowait()
                    self.logs.append(log)
            except:
                pass
            
            time.sleep(0.01)
            tentativas += 1
        
        # Salvar logs
        if self.logs:
            df = pd.DataFrame(self.logs)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            arquivo = f'logs/logs_{timestamp}.csv'
            df.to_csv(arquivo, index=False)
            print(f"[Monitor] {len(self.logs)} logs salvos em {arquivo}")
            
            # Criar também um relatório resumido
            self._gerar_relatorio_resumido(df, timestamp)
        else:
            print("[Monitor] Nenhum log para salvar")
        
        print("[Monitor] Finalizado")
    
    def _gerar_relatorio_resumido(self, df, timestamp):
        """Gera relatório textual com estatísticas"""
        try:
            # 1. RELATÓRIO DE TEXTO
            with open(f'relatorios/resumo_{timestamp}.txt', 'w', encoding='utf-8') as f:
                f.write("="*80 + "\n")
                f.write("RELATÓRIO DE EXECUÇÃO DO SISTEMA DISTRIBUÍDO\n")
                f.write("="*80 + "\n\n")
                
                # Estatísticas por tipo de evento
                f.write("EVENTOS REGISTRADOS:\n")
                f.write("-"*80 + "\n")
                contagem_eventos = df['tipo'].value_counts()
                for evento, qtd in contagem_eventos.items():
                    f.write(f"  {evento}: {qtd}\n")
                
                f.write("\n" + "="*80 + "\n")
                f.write("ESTATÍSTICAS POR SERVIDOR:\n")
                f.write("-"*80 + "\n")
                
                for servidor in df['servidor'].unique():
                    df_srv = df[df['servidor'] == servidor]
                    f.write(f"\n{servidor}:\n")
                    f.write(f"  Total de eventos: {len(df_srv)}\n")
                    
                    atendimentos = len(df_srv[df_srv['tipo'] == 'ATENDIMENTO'])
                    falhas_srv = len(df_srv[df_srv['tipo'] == 'FALHA_SERVIDOR'])
                    falhas_atend = len(df_srv[df_srv['tipo'] == 'FALHA_ATENDENTE'])
                    entradas = len(df_srv[df_srv['tipo'] == 'ENTRADA_ATENDENTE'])
                    
                    f.write(f"  Atendimentos: {atendimentos}\n")
                    f.write(f"  Falhas do servidor: {falhas_srv}\n")
                    f.write(f"  Falhas de atendentes: {falhas_atend}\n")
                    f.write(f"  Entradas de atendentes: {entradas}\n")
                
                f.write("\n" + "="*80 + "\n")
            
            print(f"[Monitor] Relatório resumido salvo em relatorios/resumo_{timestamp}.txt")
            
            # 2. TABELA DE STATUS DE SERVIDOR (requisito do professor)
            df_status = df[df['servidor'].isin(['Server-A', 'Server-B', 'Server-C'])].copy()
            df_status_resumo = df_status.groupby(['tick', 'servidor']).agg({
                'tipo': 'count'
            }).reset_index()
            df_status_resumo.columns = ['tick', 'servidor', 'eventos']
            df_status_resumo.to_csv(f'relatorios/tabela_status_{timestamp}.csv', index=False)
            print(f"[Monitor] Tabela de status salva em relatorios/tabela_status_{timestamp}.csv")
            
            # 3. TABELA DE TRANSFERÊNCIAS (requisito do professor)
            df_transferencias = df[df['tipo'].isin(['FAILOVER', 'FAILOVER_RECEBIDO', 'NOTIFICACAO_CLIENTE'])].copy()
            if not df_transferencias.empty:
                df_transferencias.to_csv(f'relatorios/tabela_transferencias_{timestamp}.csv', index=False)
                print(f"[Monitor] Tabela de transferências salva em relatorios/tabela_transferencias_{timestamp}.csv")
            
        except Exception as e:
            print(f"[Monitor] Erro ao gerar relatório: {e}")


# --- GERADOR DE GRÁFICOS ---

def gerar_graficos(estado_final, logs_df):
    """Gera gráficos de análise"""
    print("\n[Gráficos] Gerando visualizações...")
    
    if logs_df.empty:
        print("[Gráficos] ⚠ DataFrame vazio, não há dados para plotar")
        return
    
    try:
        sns.set_theme(style='whitegrid')
        fig, axes = plt.subplots(2, 2, figsize=(16, 10))
        fig.suptitle('Dashboard do Sistema Distribuído', fontsize=16, fontweight='bold')
        
        # 1. Gráfico de barras - Atendimentos por servidor
        atendimentos_por_servidor = logs_df[logs_df['tipo'] == 'ATENDIMENTO'].groupby('servidor').size()
        
        if not atendimentos_por_servidor.empty:
            cores = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6']
            atendimentos_por_servidor.plot(kind='bar', ax=axes[0, 0], color=cores[:len(atendimentos_por_servidor)])
            axes[0, 0].set_title('Total de Atendimentos por Servidor', fontweight='bold')
            axes[0, 0].set_ylabel('Atendimentos')
            axes[0, 0].set_xlabel('Servidor')
            axes[0, 0].tick_params(axis='x', rotation=45)
        else:
            axes[0, 0].text(0.5, 0.5, 'Nenhum atendimento registrado', 
                           ha='center', va='center', fontsize=12)
            axes[0, 0].set_title('Total de Atendimentos por Servidor', fontweight='bold')
        
        # 2. Gráfico de linha - Falhas ao longo do tempo
        falhas = logs_df[logs_df['tipo'] == 'FALHA_SERVIDOR']
        if not falhas.empty:
            for servidor in falhas['servidor'].unique():
                data = falhas[falhas['servidor'] == servidor].sort_values('tick')
                # Acumular falhas
                data_agrupado = data.groupby('tick').size().cumsum()
                axes[0, 1].plot(data_agrupado.index, data_agrupado.values, 
                               marker='o', label=servidor, linewidth=2)
            
            axes[0, 1].legend()
            axes[0, 1].set_title('Falhas de Servidor Acumuladas', fontweight='bold')
            axes[0, 1].set_xlabel('Tick')
            axes[0, 1].set_ylabel('Falhas Acumuladas')
            axes[0, 1].grid(True, alpha=0.3)
        else:
            axes[0, 1].text(0.5, 0.5, 'Nenhuma falha de servidor registrada', 
                           ha='center', va='center', fontsize=12)
            axes[0, 1].set_title('Falhas de Servidor ao Longo do Tempo', fontweight='bold')
        
        # 3. Gráfico de pizza - Redirecionamentos
        total_atend = len(logs_df[logs_df['tipo'] == 'ATENDIMENTO'])
        atend_com_transferencia = len(logs_df[
            (logs_df['tipo'] == 'ATENDIMENTO') & 
            (logs_df['mensagem'].str.contains('transferências', na=False)) &
            (logs_df['mensagem'].str.contains(r'\([1-9]', na=False, regex=True))
        ])
        
        if total_atend > 0:
            labels = ['Sem Redirecionamento', 'Com Redirecionamento']
            sizes = [total_atend - atend_com_transferencia, atend_com_transferencia]
            colors = ['#2ecc71', '#e74c3c']
            explode = (0, 0.1)
            
            # Só mostrar pizza se houver redirecionamentos
            if atend_com_transferencia > 0:
                axes[1, 0].pie(sizes, explode=explode, labels=labels, colors=colors,
                              autopct='%1.1f%%', shadow=True, startangle=90)
            else:
                axes[1, 0].pie([1], labels=['100% Sem Redirecionamento'], colors=['#2ecc71'],
                              shadow=True, startangle=90)
            
            axes[1, 0].set_title('Impacto das Falhas nos Atendimentos', fontweight='bold')
        else:
            axes[1, 0].text(0.5, 0.5, 'Nenhum atendimento para analisar', 
                           ha='center', va='center', fontsize=12)
            axes[1, 0].set_title('Impacto das Falhas', fontweight='bold')
        
        # 4. Estatísticas gerais
        axes[1, 1].axis('off')
        
        # Calcular estatísticas
        total_eventos = len(logs_df)
        total_falhas_servidor = len(logs_df[logs_df['tipo'] == 'FALHA_SERVIDOR'])
        total_falhas_atendente = len(logs_df[logs_df['tipo'] == 'FALHA_ATENDENTE'])
        total_entradas = len(logs_df[logs_df['tipo'] == 'ENTRADA_ATENDENTE'])
        total_failovers = len(logs_df[logs_df['tipo'] == 'FAILOVER'])
        
        stats_text = f"""
        ESTATÍSTICAS GERAIS
        {'='*40}
        
        Total de Eventos: {total_eventos:,}
        
        Atendimentos: {total_atend:,}
        Falhas de Servidor: {total_falhas_servidor}
        Falhas de Atendentes: {total_falhas_atendente}
        Substituições: {total_entradas}
        Failovers: {total_failovers}
        
        Com Redirecionamento: {atend_com_transferencia}
        Taxa de Redirecionamento: {(atend_com_transferencia/total_atend*100 if total_atend > 0 else 0):.2f}%
        """
        
        axes[1, 1].text(0.1, 0.5, stats_text, fontsize=11, family='monospace',
                       verticalalignment='center', bbox=dict(boxstyle='round', 
                       facecolor='wheat', alpha=0.5))
        axes[1, 1].set_title('Resumo Executivo', fontweight='bold')
        
        plt.tight_layout()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        arquivo_grafico = f'graficos/dashboard_{timestamp}.png'
        plt.savefig(arquivo_grafico, dpi=150, bbox_inches='tight')
        plt.close()
        
        print(f"[Gráficos] ✅ Dashboard salvo em {arquivo_grafico}")
        
    except Exception as e:
        print(f"[Gráficos] ❌ Erro ao gerar gráficos: {e}")
        import traceback
        traceback.print_exc()


# --- MAIN ---

def main():
    print("="*80)
    print("SISTEMA DISTRIBUÍDO DE ATENDIMENTO AO CLIENTE")
    print("="*80)
    print(f"\nConfiguração:")
    print(f"  Timesteps: {CONFIG['TIMESTEPS']:,}")
    print(f"  Requisições/tick: {CONFIG['MIN_REQ_PER_TICK']}-{CONFIG['MAX_REQ_PER_TICK']}")
    print(f"  Buffer size: {CONFIG['BUFFER_SIZE']}")
    print(f"  Min atendentes/tipo: {CONFIG['MIN_ATENDENTES_POR_TIPO']}")
    print(f"  Delay por tick: {CONFIG['DELAY_TICK']*1000:.1f}ms")
    print(f"  Prob. falha servidor: {CONFIG['PROB_FALHA_SERVIDOR']*100:.1f}%")
    print()
    
    # TESTE DE SANIDADE: Verificar se classes funcionam
    print("🔍 Executando testes de sanidade...")
    try:
        # Teste 1: Criar atendente
        atendente_teste = Atendente("TEST-001", "suporte", "Server-Test")
        assert atendente_teste.ativo == True
        print("  ✅ Classe Atendente OK")
        
        # Teste 2: Criar requisição
        req_teste = Requisicao(1, "vendas", 0)
        assert req_teste.tipo == "vendas"
        req_dict = req_teste.to_dict()
        req_restaurada = Requisicao.from_dict(req_dict)
        assert req_restaurada.id == 1
        print("  ✅ Classe Requisicao OK")
        
        # Teste 3: Verificar multiprocessing
        from multiprocessing import cpu_count
        print(f"  ✅ Multiprocessing OK ({cpu_count()} CPUs disponíveis)")
        
        print("\n✅ Todos os testes passaram! Iniciando sistema...\n")
        time.sleep(1)
        
    except Exception as e:
        print(f"\n❌ ERRO nos testes: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Manager para compartilhar dados
    manager = Manager()
    evento_shutdown = Event()
    estado_compartilhado = manager.dict()
    
    # Filas de comunicação
    fila_logs = Queue()
    fila_failover_global = Queue()
    
    # Configuração dos servidores
    servidores_config = [
        ('Server-A', 500),
        ('Server-B', 700),
        ('Server-C', 1000)
    ]
    
    # Criar filas para cada servidor
    filas_entrada = {}
    filas_failover_in = {}
    
    for nome, _ in servidores_config:
        filas_entrada[nome] = Queue()
        filas_failover_in[nome] = Queue()
    
    # Criar processos de servidores
    processos_servidores = []
    for nome, capacidade in servidores_config:
        servidor = ServidorDistribuido(
            nome=nome,
            capacidade_atendentes=capacidade,
            config=CONFIG,
            fila_entrada=filas_entrada[nome],
            fila_logs=fila_logs,
            fila_failover_out=fila_failover_global,
            fila_failover_in=filas_failover_in[nome],
            evento_shutdown=evento_shutdown,
            estado_compartilhado=estado_compartilhado
        )
        processos_servidores.append(servidor)
        estado_compartilhado[nome] = {'ativo': True}
    
    # Load Balancer
    load_balancer = LoadBalancer(
        filas_servidores=filas_entrada,
        fila_logs=fila_logs,
        evento_shutdown=evento_shutdown,
        estado_compartilhado=estado_compartilhado,
        config=CONFIG
    )
    
    # Failover Coordinator
    filas_com_failover = {
        nome: {'entrada': filas_entrada[nome], 'failover': filas_failover_in[nome]}
        for nome in filas_entrada.keys()
    }
    
    failover_coord = FailoverCoordinator(
        fila_failover=fila_failover_global,
        filas_servidores=filas_com_failover,
        fila_logs=fila_logs,
        evento_shutdown=evento_shutdown,
        estado_compartilhado=estado_compartilhado,
        config=CONFIG
    )
    
    # Monitor
    monitor = MonitorLogs(fila_logs, evento_shutdown)
    
    # Iniciar todos os processos
    print("Iniciando processos distribuídos...\n")
    
    for servidor in processos_servidores:
        servidor.start()
    
    load_balancer.start()
    failover_coord.start()
    monitor.start()
    
    # Monitoramento em tempo real
    try:
        print(f"Sistema rodando com {len(processos_servidores) + 3} processos...")
        print("Pressione Ctrl+C para encerrar\n")
        
        tick_inicial = 0
        while not evento_shutdown.is_set():
            time.sleep(5)
            
            # Pegar tick atual de qualquer servidor
            tick_atual = 0
            for estado in estado_compartilhado.values():
                if estado.get('tick', 0) > tick_atual:
                    tick_atual = estado.get('tick', 0)
            
            # Status a cada 5 segundos
            print("\n" + "="*80)
            print(f"STATUS DO SISTEMA (Tick {tick_atual}/{CONFIG['TIMESTEPS']})")
            print("="*80)
            
            total_atendimentos = 0
            total_filas = 0
            servidores_ativos = 0
            
            for nome, estado in estado_compartilhado.items():
                status = "🟢 ATIVO" if estado.get('ativo') else "🔴 INATIVO"
                if estado.get('ativo'):
                    servidores_ativos += 1
                    
                print(f"\n{nome}: {status} (PID: {estado.get('pid', 'N/A')})")
                print(f"  Atendimentos: {estado.get('total_atendimentos', 0):,}")
                print(f"  Filas: Suporte={estado.get('fila_suporte', 0)}, " +
                      f"Vendas={estado.get('fila_vendas', 0)}")
                print(f"  Atendentes Ativos: Suporte={estado.get('atendentes_suporte_ativos', 0)}, " +
                      f"Vendas={estado.get('atendentes_vendas_ativos', 0)}")
                print(f"  Falhas do Servidor: {estado.get('falhas_servidor', 0)}")
                
                total_atendimentos += estado.get('total_atendimentos', 0)
                total_filas += estado.get('total_fila', 0)
            
            print(f"\nTOTAL DO SISTEMA:")
            print(f"  Servidores Ativos: {servidores_ativos}/{len(estado_compartilhado)}")
            print(f"  Atendimentos Totais: {total_atendimentos:,}")
            print(f"  Filas Pendentes: {total_filas}")
            print(f"  Progresso: {(tick_atual/CONFIG['TIMESTEPS']*100):.1f}%")
            print("="*80)
            
            # Verificar se completou
            if tick_atual >= CONFIG['TIMESTEPS']:
                print("\n✅ SIMULAÇÃO COMPLETADA!")
                break
            
            # Verificar se está travado
            if tick_atual == tick_inicial:
                print("\n⚠ AVISO: Tick não está avançando! Sistema pode estar travado.")
            tick_inicial = tick_atual
    
    except KeyboardInterrupt:
        print("\n\n⚠ Encerrando sistema...")
    
    # Shutdown gracioso
    evento_shutdown.set()
    
    print("\nAguardando finalização dos processos...")
    for servidor in processos_servidores:
        servidor.join(timeout=5)
    
    load_balancer.join(timeout=5)
    failover_coord.join(timeout=5)
    
    # IMPORTANTE: Aguardar monitor salvar logs antes de continuar
    print("Salvando logs...")
    monitor.join(timeout=10)  # Dar mais tempo para salvar
    time.sleep(2)  # Garantir que arquivo foi escrito
    
    # Relatório final
    print("\n" + "="*80)
    print("RELATÓRIO FINAL")
    print("="*80)
    
    total_atendimentos_final = 0
    total_falhas_servidor = 0
    
    for nome, estado in estado_compartilhado.items():
        print(f"\n{nome}:")
        print(f"  Status Final: {'ATIVO' if estado.get('ativo') else 'INATIVO'}")
        print(f"  Total Atendimentos: {estado.get('total_atendimentos', 0):,}")
        print(f"  Falhas do Servidor: {estado.get('falhas_servidor', 0)}")
        print(f"  Fila Final: Suporte={estado.get('fila_suporte', 0)}, " +
              f"Vendas={estado.get('fila_vendas', 0)}")
        
        total_atendimentos_final += estado.get('total_atendimentos', 0)
        total_falhas_servidor += estado.get('falhas_servidor', 0)
    
    print(f"\n{'='*80}")
    print(f"TOTAIS:")
    print(f"  Atendimentos Realizados: {total_atendimentos_final:,}")
    print(f"  Falhas de Servidor: {total_falhas_servidor}")
    print("="*80)
    
    # Gerar gráficos se houver logs
    print("\nGerando relatórios e gráficos...")
    
    # Tentar encontrar o arquivo de log mais recente
    try:
        import glob
        arquivos_log = glob.glob('logs/logs_*.csv')
        
        if arquivos_log:
            # Pegar o arquivo mais recente
            arquivo_mais_recente = max(arquivos_log, key=os.path.getctime)
            print(f"Carregando logs de: {arquivo_mais_recente}")
            
            df_logs = pd.read_csv(arquivo_mais_recente)
            print(f"Total de {len(df_logs)} eventos registrados")
            
            gerar_graficos(dict(estado_compartilhado), df_logs)
            print("✅ Gráficos gerados com sucesso!")
        else:
            print("⚠ Nenhum arquivo de log encontrado")
            
    except Exception as e:
        print(f"⚠ Erro ao gerar gráficos: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n✅ Sistema finalizado com sucesso!")
    print("\nArquivos gerados:")
    print("  📁 logs/ - Logs detalhados em CSV")
    print("  📊 graficos/ - Visualizações em PNG")


if __name__ == "__main__":
    # Necessário para Windows
    mp.set_start_method('spawn', force=True)
    
    # Perguntar ao usuário
    print("\n" + "="*80)
    print("MODO DE EXECUÇÃO")
    print("="*80)
    print("\n1. Executar um cenário único")
    print("2. Executar todos os cenários (comparação)")
    print("3. Executar cenário personalizado")
    
    try:
        escolha = input("\nEscolha (1-3) [default=1]: ").strip() or "1"
        
        if escolha == "1":
            # Cenário único
            print("\nCenários disponíveis:")
            for i, nome in enumerate(CENARIOS.keys(), 1):
                print(f"  {i}. {nome}")
            
            cenario_idx = input(f"\nEscolha o cenário (1-{len(CENARIOS)}) [default=1]: ").strip() or "1"
            cenario_nome = list(CENARIOS.keys())[int(cenario_idx) - 1]
            CONFIG = CENARIOS[cenario_nome]
            
            print(f"\n▶ Executando cenário: {cenario_nome}\n")
            main()
        
        elif escolha == "2":
            # Múltiplos cenários
            print("\n▶ Executando TODOS os cenários para comparação...\n")
            
            resultados_comparacao = []
            
            for nome_cenario, config in CENARIOS.items():
                print(f"\n{'='*80}")
                print(f"EXECUTANDO: {nome_cenario}")
                print(f"{'='*80}\n")
                
                CONFIG = config
                
                # Executar cenário
                try:
                    # Rodar main() mas capturar resultado
                    # (simplificado - em produção, refatorar main para retornar dados)
                    main()
                    
                    print(f"\n✅ {nome_cenario} completado!\n")
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"\n❌ {nome_cenario} falhou: {e}\n")
            
            print("\n" + "="*80)
            print("COMPARAÇÃO CONCLUÍDA")
            print("="*80)
            print("\nVerifique as pastas logs/, graficos/ e relatorios/")
            print("para comparar os resultados de cada cenário.")
        
        elif escolha == "3":
            # Cenário personalizado
            print("\n▶ Configuração Personalizada\n")
            
            CONFIG = {
                'TIMESTEPS': int(input("  Timesteps (default=1000): ") or "1000"),
                'MIN_REQ_PER_TICK': int(input("  Min req/tick (default=10): ") or "10"),
                'MAX_REQ_PER_TICK': int(input("  Max req/tick (default=50): ") or "50"),
                'BUFFER_SIZE': int(input("  Buffer size (default=5000): ") or "5000"),
                'PROB_FALHA_SERVIDOR': float(input("  Prob. falha servidor (default=0.01): ") or "0.01"),
                'PROB_FALHA_ATENDENTE': float(input("  Prob. falha atendente (default=0.001): ") or "0.001"),
                'TEMPO_RECOVERY_SERVIDOR': int(input("  Tempo recovery (default=20): ") or "20"),
                'MIN_ATENDENTES_POR_TIPO': 100,
                'FALHAS_PROGRAMADAS': [200, 500, 800],
                'DELAY_TICK': 0.01
            }
            
            print("\n▶ Executando configuração personalizada...\n")
            main()
        
    except KeyboardInterrupt:
        print("\n\n⚠ Execução cancelada pelo usuário")
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()