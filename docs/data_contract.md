# Contrato de dados — Olist

## Objetivo

Registrar o que cada CSV representa e as regras que o pipeline local deve aplicar ao carregar e preparar os dados.

## Referências

- Modelo e descrições da Olist: https://www.kaggle.com/code/anshumoudgil/olist-ecommerce-analytics-quasi-poisson-poly-regs/script

## Regras gerais do pipeline

- Os arquivos em `data/source` são a cópia de origem. A camada `raw` no MinIO deve preservar seu conteúdo, sem limpeza ou alteração.
- Colunas de identificação, como `order_id` e `product_id`, devem ser tratadas como texto.
- Prefixos de CEP devem ser texto com cinco caracteres nas camadas tratadas. A versão original permanece preservada em `raw`.
- Datas devem ser convertidas para data/hora nas camadas tratadas. Valores que não puderem ser interpretados devem ser sinalizados.
- Valores ausentes devem continuar ausentes. Não preencher com zero ou inventar informação sem uma regra de negócio.
- Problemas detectados devem ser registrados como exceções para revisão. Não excluir automaticamente linhas da origem.
- Não juntar diretamente itens, pagamentos e avaliações para calcular valores por pedido. Primeiro agregar cada tabela filha no grão necessário.

## Arquivos e granularidade

| Arquivo | O que cada linha representa | Chave candidata observada | Regras ou observações |
|---|---|---|---|
| `olist_customers_dataset.csv` | Um registro identificado por `customer_id` | `customer_id` | `customer_unique_id` se repete e não deve ser usado sozinho como chave primária. |
| `olist_geolocation_dataset.csv` | Um registro de localização associado a um prefixo de CEP | Nenhuma chave única por CEP | Um prefixo pode ter muitas linhas. Não juntar diretamente à tabela de clientes ou vendedores sem antes definir uma regra de agregação. |
| `olist_orders_dataset.csv` | Um pedido | `order_id` | Datas ausentes podem depender do status do pedido. Validar as datas considerando o status. |
| `olist_order_items_dataset.csv` | Um item dentro de um pedido | `order_id` + `order_item_id` | Cada pedido pode ter vários itens. Preço e frete pertencem ao grão do item. |
| `olist_order_payments_dataset.csv` | Um pagamento ou sequência de pagamento de um pedido | `order_id` + `payment_sequential` | Um pedido pode ter vários registros de pagamento. |
| `olist_order_reviews_dataset.csv` | Uma avaliação associada a um pedido | `review_id` + `order_id`, candidata nesta cópia | `review_id` sozinho se repete; um pedido pode ter várias avaliações. Título e comentário podem estar vazios. |
| `olist_products_dataset.csv` | Um produto | `product_id` | Há produtos sem categoria e categorias sem tradução. Preservar o valor original e sinalizar o que não estiver mapeado. |
| `olist_sellers_dataset.csv` | Um vendedor | `seller_id` | O prefixo de CEP pode não encontrar correspondência na geolocalização. |
| `product_category_name_translation.csv` | Uma tradução para uma categoria | `product_category_name` | O arquivo tem BOM UTF-8 no cabeçalho. O leitor deve removê-lo corretamente. |

## Verificações iniciais

Na ingestão, conferir se os nove arquivos esperados chegaram e registrar nome, quantidade de linhas e hash de cada arquivo.

Na preparação, verificar:

- duplicidade das chaves candidatas;
- referências entre pedidos, clientes, itens, produtos, vendedores, pagamentos e avaliações;
- valores ausentes e datas que não puderem ser interpretadas;
- CEPs sem correspondência geográfica;
- categorias sem tradução;
- valores fora dos domínios esperados, como métodos de pagamento desconhecidos.

## Pendências e hipóteses

- Os prefixos de CEP têm várias coordenadas. A regra para escolher uma coordenada por CEP ainda precisa ser definida conforme o uso analítico.
- Diferenças entre pagamentos e valores dos itens devem ser reportadas para investigação. Não presumir automaticamente que sejam erros.
- Datas fora da sequência esperada devem ser sinalizadas. Não alterar nem excluir esses registros sem entender a causa.

## Decisão para esta versão

Este contrato descreve as regras técnicas iniciais da V1. Decisões que dependam do significado comercial dos dados ficam registradas como pendentes até serem validadas.