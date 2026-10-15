# Airflow local — orquestração da V2

## Arquitetura

O perfil airflow adiciona Apache Airflow 3.3.2 com CeleryExecutor, API server, scheduler, DAG processor, worker, Redis e PostgreSQL exclusivo para metadados. O PostgreSQL analítico continua separado. As tarefas usam o provider Docker para iniciar a imagem existente olist-python-jobs:local; Parquets e manifestos continuam passando diretamente pelo MinIO.

A DAG olist_batch_v2 é manual (schedule=None). A ingestão calcula o BATCH_ID pelo conteúdo dos CSVs e imprime o identificador puro como última linha. DockerOperator guarda somente essa linha em XCom; as tarefas processing, curated e warehouse_load recebem BATCH_ID como variável de ambiente. A ingestão recebe credenciais do MinIO; apenas warehouse_load recebe também as credenciais do PostgreSQL. Nenhum dado de arquivo passa pelo Airflow.

O worker acessa a Execution API pelo DNS interno airflow-api-server:8080 e o socket Docker local. Ele monta OLIST_SOURCE_DIR somente para leitura no container de ingestão. Os containers de tarefa conectam-se à rede olist-local_pipeline e usam minio:9000 e postgres:5432.

## Pré-requisitos e configuração

1. Docker Desktop com Docker Compose v2 e memória suficiente para os serviços Airflow.
2. Configure .env seguindo .env.example. OLIST_SOURCE_DIR deve ser um caminho absoluto visível ao Docker Desktop; o valor de exemplo aponta para data/source deste repositório neste computador.
3. Configure MINIO_ROOT_USER, MINIO_ROOT_PASSWORD e POSTGRES_PASSWORD como na execução atual. Defina também AIRFLOW_DB_PASSWORD, AIRFLOW_FERNET_KEY, AIRFLOW_API_SECRET_KEY e AIRFLOW_JWT_SECRET no .env; o Compose não aceita valores padrão para esses segredos. Gere valores aleatórios no PowerShell: use 32 bytes em hexadecimal para AIRFLOW_DB_PASSWORD e AIRFLOW_API_SECRET_KEY, 32 bytes em Base64 para AIRFLOW_FERNET_KEY e 64 bytes em Base64 para AIRFLOW_JWT_SECRET. Não versione .env.
4. Disponibilize os nove CSVs em data/source/, inicie MinIO, crie o bucket e inicialize o schema V2 no PostgreSQL se necessário.

## Iniciar e executar

Na raiz do repositório:

    docker compose --profile jobs build
    docker compose --profile airflow up airflow-init
    docker compose --profile airflow up -d airflow-api-server airflow-scheduler airflow-dag-processor airflow-worker

Abra http://localhost:8080 e entre com o usuário admin. A senha fica em /opt/airflow/auth/passwords.json, dentro do volume Docker nomeado airflow_auth, persistido mesmo quando os containers são recriados. Para consultar localmente: docker compose --profile airflow exec airflow-api-server cat /opt/airflow/auth/passwords.json. Não remova o volume airflow_auth ao limpar os serviços; `down --volumes` apaga essa credencial junto com os volumes do Airflow. A DAG olist_batch_v2 já fica habilitada; execute-a manualmente pela interface. Cada execução processa os arquivos atualmente em data/source/; se o conteúdo mudar, a ingestão produz outro BATCH_ID.

A interface fica publicada somente em 127.0.0.1. Para parar os serviços Airflow sem parar MinIO, PostgreSQL analítico ou Metabase:

    docker compose --profile airflow down

Não use down --volumes se quiser preservar o banco de metadados do Airflow.

## Falhas e reexecução

Uma falha no container falha a tarefa Airflow e impede a execução das etapas seguintes. Corrija a causa e tente novamente o mesmo lote pela interface. max_active_runs=1 evita duas execuções simultâneas da DAG. As tentativas criam manifestos próprios; algumas saídas continuam em caminhos estáveis por BATCH_ID, então não execute os mesmos scripts em paralelo ao Airflow.

warehouse_load continua transacional e limitado ao lote. Uma repetição do mesmo lote não deve duplicar seus registros. Alterar a origem produz um novo BATCH_ID sem substituir objetos de outro lote.

## Limites locais

O socket Docker dá ao Airflow controle amplo sobre o Docker Engine local. Esta configuração destina-se a desenvolvimento individual, não a execução compartilhada ou produção. O primeiro fluxo não tem agendamento automático nem permite começar diretamente por um BATCH_ID existente no MinIO.