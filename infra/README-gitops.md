# GitOps com ArgoCD + Minikube - Guia Completo

> **Referência histórica.** O fluxo operacional canônico é o Caminho B do
> `README.md` raiz e os scripts em `scripts/minikube/`. Comandos e a seção
> “Status Atual” abaixo preservam contexto anterior e não promovem o estado
> presente da plataforma.

Este guia documenta o processo completo de setup da infraestrutura de dados com ArgoCD, Minikube e Helm Charts. Permite que qualquer pessoa execute o projeto **Data-Master-Platform** localmente em sua máquina.

## Pré-requisitos

Antes de começar, instale e configure as seguintes ferramentas:

### 1. **Docker Desktop** (Obrigatório para Minikube)
- **Download**: [https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop)
- **Instalação no Windows**:
  1. Baixe o instalador.
  2. Execute como administrador.
  3. Siga o assistente de instalação.
  4. **Importante**: Inicie o Docker Desktop antes de usar Minikube.
- **Verificação**: Abra o terminal e execute `docker --version`. Deve retornar a versão instalada.

### 2. **Git**
- **Download**: [https://git-scm.com/downloads](https://git-scm.com/downloads)
- **Instalação no Windows**: Siga o assistente padrão.
- **Verificação**: `git --version`

### 3. **kubectl** (CLI do Kubernetes)
- **Download**: [https://kubernetes.io/docs/tasks/tools/](https://kubernetes.io/docs/tasks/tools/)
- **Instalação no Windows**:
  1. Baixe o binário `kubectl.exe`.
  2. Adicione ao PATH do sistema (veja seção de Troubleshooting).
- **Verificação**: `kubectl version --client`

### 4. **Minikube** (Cluster Kubernetes local)
- **Download**: [https://minikube.sigs.k8s.io/docs/start/](https://minikube.sigs.k8s.io/docs/start/)
- **Instalação no Windows**:
  1. Baixe o instalador `.exe`.
  2. Execute e siga o assistente.
- **Verificação**: `minikube version`

### 5. **Helm** (Gerenciador de pacotes Kubernetes)
- **Download**: [https://helm.sh/docs/intro/install/](https://helm.sh/docs/intro/install/)
- **Instalação no Windows**:
  1. Baixe o binário `helm.exe`.
  2. Adicione ao PATH (veja Troubleshooting).
- **Verificação**: `helm version`

### 6. **SSH para GitHub** (Opcional, mas recomendado)
- Gere uma chave SSH: `ssh-keygen -t rsa -b 4096 -C "seu-email@exemplo.com"`
- Adicione a chave pública (`id_rsa.pub`) em: [https://github.com/settings/keys](https://github.com/settings/keys)
- **Verificação**: `ssh -T git@github.com` (deve conectar sem senha)

## Etapa 1: Clonar o Repositório

```bash
git clone git@github.com:ale468/Data-Master-Platform.git
cd Data-Master-Platform
```

## Etapa 2: Iniciar o Minikube

**Importante**: Certifique-se de que o Docker Desktop está rodando!

```bash
minikube start --driver=docker
```

- **Verificação**: `minikube status` deve mostrar "Running".
- **Se der erro**: Veja a seção de Troubleshooting abaixo.

## Etapa 3: Instalar o ArgoCD

```bash
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
```

- **Aguarde**: Pode levar 1-2 minutos para os pods ficarem prontos.
- **Verificação**: `kubectl get pods -n argocd`

## Etapa 4: Acessar a Interface do ArgoCD

```bash
kubectl port-forward svc/argocd-server -n argocd 8080:443
```

- **Acesse**: [http://localhost:8080](http://localhost:8080)
- **Usuário**: `admin`
- **Senha**:
  ```bash
  kubectl -n argocd get secret argocd-initial-admin-secret \
    -o jsonpath="{.data.password}" | base64 -d
  ```

## Etapa 5: Registrar Repositório Git no ArgoCD

**Nota Importante**: Devido a limitações de autenticação, recomendamos usar HTTPS para repositórios acessíveis pelo ambiente local. O ArgoCD funciona melhor com HTTPS do que SSH neste setup.

Via interface web (recomendado):
1. Acesse http://localhost:8080 (porta 8080 ou 8081)
2. Login: admin / senha do passo anterior
3. Vá em "Settings" → "Repositories"
4. Clique "Connect Repo"
5. Selecione "Via HTTPS" e insira: `https://github.com/ale468/Data-Master-Platform.git`
6. Deixe campos de autenticação vazios quando o ambiente não exigir credenciais

Via CLI (opcional):
```bash
argocd login localhost:8081 --username admin --password <SENHA-ACIMA>
# Para repositorios HTTPS acessiveis pelo cluster, geralmente nao precisa registrar explicitamente
```

## Etapa 6: Aplicar App-of-Apps (Aplicação Raiz)

```bash
kubectl apply -f infra/argocd/applications/root/app-of-apps.yaml
```

Esta aplicação raiz gerencia automaticamente todas as outras aplicações com a seguinte ordem de implantação:

1. **Wave 0**: Spark Operator (CRDs + Controller)
2. **Wave 1**: MinIO (Armazenamento)
3. **Wave 2**: Jupyter Notebook (Ambiente de desenvolvimento)
4. **Wave 3**: Spark Jobs (Workloads de exemplo)

## Etapa 7: Verificar Sincronização Automática

O ArgoCD irá sincronizar automaticamente todas as aplicações na ordem correta. Monitore o progresso:

```bash
# Ver todas as aplicações
kubectl get applications -n argocd

# Ver status detalhado
argocd app list

# Ver logs de sincronização (se necessário)
kubectl logs -n argocd deployment/argocd-application-controller
```

## Etapa 8: Verificar Recursos Implantados

```bash
# Spark Operator
kubectl get pods -n spark-operator
kubectl get crd | grep spark

# MinIO
kubectl get pods -n minio
kubectl get pvc -n minio

# Jupyter
kubectl get pods -n jupyter

# Spark Jobs
kubectl get pods -n spark-jobs
kubectl get sparkapplication -n spark-jobs
```

```bash
kubectl get pods -n spark-operator
kubectl get pods -n spark-jobs
kubectl get pods -n jupyter
kubectl get sparkapplication -n spark-jobs
```

## Etapa 10: Executar PySpark Interativo (Databricks-like)

Para uma experiência similar ao Databricks:

```bash
kubectl run pyspark-shell --image=apache/spark-py:v3.3.1 --rm -it -- /opt/spark/bin/pyspark
```

**Exemplos de comandos no shell PySpark:**
```python
# Criar DataFrame
df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
df.show()

# Filtrar dados
df.filter(df.age > 26).show()

# SQL
df.createOrReplaceTempView("people")
spark.sql("SELECT * FROM people WHERE age > 28").show()
```

## Etapa 11: Acessar Jupyter Notebook

```bash
kubectl port-forward svc/jupyter -n jupyter 8888:8888
```

- **Acesse**: [http://localhost:8888](http://localhost:8888)
- **Token**: `spark123`
- **Interface**: Use o token para fazer login e começar a trabalhar com notebooks PySpark.

## Etapa 9: Acessar Serviços

- **ArgoCD UI**: `kubectl port-forward svc/argocd-server -n argocd 8080:443`
- **MinIO Console**: `kubectl port-forward svc/minio -n minio 9001:9001` (usuário: minio, senha: minio123)
- **MinIO API**: `kubectl port-forward svc/minio -n minio 9000:9000`
- **Jupyter Notebook**: `kubectl port-forward svc/jupyter -n jupyter 8888:8888`
- **Spark UI** (se disponível): `kubectl port-forward svc/spark -n spark-operator 8088:8080`

## Limitações e Decisões Técnicas

### ArgoCD e Autenticação Git
- **Status**: **Resolvido** - Todas as aplicações agora usam HTTPS e estão integradas ao ArgoCD
- **Solução implementada**: App-of-apps com sync waves garante ordem de implantação
- **Benefício**: Deploy completamente automatizado e versionado

### Spark Operator
- **Status**: **Corrigido** - Chart real implementado com CRDs, RBAC e controller
- **Melhorias**: ServiceAccount, ClusterRole, probes de saúde
- **Resultado**: Spark Operator funcional com CRDs instaladas

### Jupyter em Kubernetes
- **Status**: **Corrigido** - Namespace dinâmico, probes adicionadas, labels padronizadas
- **Melhorias**: Liveness/Readiness probes, resources configuráveis
- **Resultado**: Chart reutilizável e resiliente

### MinIO Integration
- **Status**: **Implementado** - Chart completo com PVC, probes e segurança básica
- **Melhorias**: Persistência via PVC, serviceAccount, securityContext
- **Resultado**: MinIO integrado ao fluxo GitOps com armazenamento persistente

### Dependências entre Aplicações
- **Status**: **Implementado** - Sync waves garantem ordem: Operator → MinIO → Jupyter → Jobs
- **Benefício**: Deploy previsível e sem conflitos de dependência

### Recursos do Minikube
- **Limitação**: Ambiente local tem recursos limitados
- **Otimização**: Configurações de CPU/memory ajustadas para estabilidade

## Troubleshooting (Problemas Comuns)

### 1. **Erro: "minikube start" falha com "docker daemon not running"**
- **Causa**: Docker Desktop não está iniciado.
- **Solução**:
  1. Abra o Docker Desktop.
  2. Aguarde até aparecer "Docker is running".
  3. Execute `minikube start --driver=docker` novamente.

### 2. **Erro: "kubectl: command not found"**
- **Causa**: kubectl não está no PATH.
- **Solução (Windows)**:
  1. Baixe `kubectl.exe` de [https://kubernetes.io/docs/tasks/tools/](https://kubernetes.io/docs/tasks/tools/).
  2. Mova o arquivo para `C:\Windows\System32` ou adicione ao PATH:
     - Pressione Win + X → "Configurações do Sistema" → "Sobre" → "Configurações avançadas do sistema" → "Variáveis de ambiente".
     - Em "Variáveis do sistema", edite "Path" e adicione o caminho da pasta do kubectl (ex.: `C:\kubectl`).
  3. Reinicie o terminal e verifique: `kubectl version --client`.

### 3. **Erro: "helm: command not found"**
- Mesmo problema do kubectl. Adicione ao PATH conforme acima.

### 4. **Erro: "etcdserver: request timed out"**
- **Causa**: Cluster sobrecarregado (muitos pods problemáticos).
- **Solução**:
  1. Delete deployments problemáticos: `kubectl delete deployment <nome> -n <namespace>`
  2. Limpe pods: `kubectl delete pods --field-selector=status.phase=Failed`
  3. Reinicie Minikube: `minikube stop && minikube start --driver=docker`

### 5. **Erro: "Unable to connect to the server: EOF"**
- **Causa**: Minikube não está totalmente iniciado.
- **Solução**:
  1. Verifique status: `minikube status`
  2. Se apiserver parado: `minikube start --driver=docker`
  3. Aguarde e teste: `kubectl get nodes`

### 6. **Pods ficam em "Pending" ou "ErrImagePull"**
- Verifique se o Docker está rodando (veja erro 1).
- Execute `minikube image load <imagem>` se necessário.
- Verifique conectividade: `docker pull hello-world`.

### 7. **ArgoCD não sincroniza ou "repo not found"**
- Verifique se a chave SSH está configurada: `ssh -T git@github.com`.
- No ArgoCD, certifique-se de que o repositório foi adicionado corretamente.
- Execute `argocd app get <app-name>` para detalhes do erro.

### 8. **Spark Operator não processa aplicações**
- Verifique se o Operator está rodando: `kubectl get pods -n spark-operator`
- Verifique logs: `kubectl logs -n spark-operator deployment/spark-operator-controller`
- Certifique-se de que a ServiceAccount existe no namespace correto.

### 9. **Problemas Gerais**
- **Logs do Minikube**: `minikube logs`
- **Reiniciar Minikube**: `minikube delete && minikube start --driver=docker`
- **Ver status completo**: `minikube status`
- **Limpar cache**: `docker system prune -a` (cuidado, remove tudo)

### 10. **Validação da Instalação Completa**
Execute estes comandos para verificar se tudo está funcionando:

```bash
# Verificar cluster
kubectl get nodes
kubectl get pods -A

# Verificar ArgoCD
kubectl get pods -n argocd
kubectl port-forward svc/argocd-server -n argocd 8080:443

# Verificar Spark Operator
kubectl get pods -n spark-operator
kubectl get sparkapplications -A

# Verificar Jupyter
kubectl get pods -n jupyter
kubectl port-forward svc/jupyter -n jupyter 8888:8888

# Testar PySpark (opcional)
kubectl exec -it deployment/jupyter -n jupyter -- /bin/bash
# Dentro do container: python -c "import pyspark; print('PySpark OK')"
```

## Recursos Adicionais

- [Documentação ArgoCD](https://argo-cd.readthedocs.io/)
- [Documentação Minikube](https://minikube.sigs.k8s.io/docs/)
- [Documentação Helm](https://helm.sh/docs/)
- [Documentação Apache Spark](https://spark.apache.org/docs/latest/)

## Próximos Passos e Melhorias

### Funcionalidades Planejadas
- **MinIO Integration**: Armazenamento de dados distribuído
- **Spark Jobs Avançados**: Workflows complexos com múltiplas etapas
- **Monitoring**: Métricas e dashboards com Prometheus/Grafana
- **CI/CD Pipeline**: Automação completa de deploy

### Melhorias Técnicas
- **ArgoCD Full GitOps**: Resolver autenticação para todos componentes
- **Security**: Configurações de RBAC e secrets management
- **Scalability**: Configurações para clusters maiores
- **Backup/Restore**: Estratégias para dados persistentes

### Como Contribuir
1. Teste as configurações atuais
2. Reporte bugs ou melhorias via Issues
3. Proponha mudanças via Pull Requests
4. Documente novas funcionalidades

---

**Status Atual do Projeto:**
- Minikube + Kubernetes funcionando
- ArgoCD instalado e configurado (com app-of-apps)
- Spark Operator real instalado (com CRDs e RBAC)
- MinIO integrado ao GitOps (com PVC e probes)
- Jupyter Notebook corrigido (namespace dinâmico + probes)
- PySpark interativo funcionando
- Jobs Spark com RBAC adequado
- Sync waves implementados para ordem de deploy
- Charts padronizados com helpers e labels

**Arquitetura Final:**
1. **App-of-Apps** gerencia todas as aplicações
2. **Sync Waves** garantem ordem: Spark Operator → MinIO → Jupyter → Spark Jobs
3. **Charts padronizados** com helpers, probes e segurança
4. **RBAC adequado** para Spark jobs
5. **Persistência** via PVC para MinIO
6. **Comunicação interna** validada entre serviços

**Dúvidas?** Abra uma issue no repositório ou consulte a documentação principal em `README.md`.
