<img src="https://raw.githubusercontent.com/Databricks-BR/lab_agosto_2025/main/images/head_lab.png">

## LAB 02 - Entendendo os dados com o GENIE

### 01. Crie um Genie Space associando as tabelas gerado pelo lab anterior

### 02. Perguntas testes:
```
qual a quantidade de clientes inadimplentes?
```
```
Quais são as empresas de porte grande?
```

### 03. Add uma DIMENSION 'Porte de Empresa' e refaça a pergunta (Quais são as empresas de porte grande?)
```
CASE
WHEN empresas_sp.val_capital_social < 50000 THEN 'Pequena'
WHEN empresas_sp.val_capital_social BETWEEN 50000 AND 500000 THEN 'Média'
ELSE 'Grande'
END AS porte_empresa
```

### 04. Nomeie, add descrição e add recomendações de perguntas
```
Nome: AGENTE INADIMPLENCIA <$Suas Iniciais>
```
```
Descrição: Agente com dados de inadimplência do IBGE de São Paulo
```
```
Sample questions:
```
```
valor total das faturas em aberto?
```
```
quantidade de faturas em aberto?
```
```
quantidade de faturas em aberto por aging da dívida?
```
```
quantidade de faturas em aberto por faixa de atraso?
```
```
quais os 10 bairros com maior quantidade de faturas em aberto com aging da divida a cima de 60 dias?
```

### 05. Add General Instructions
```
* Você é um analista sênior de risco e crédito focado no mercado de São Paulo. Sua missão é ajudar stakeholders a entender padrões de inadimplência, perfil de empresas e oportunidades de mercado.

* Sempre responda em Português do Brasil.

* Ao analisar valores monetários, formate como moeda (R$).

* A coluna aging_divida representa os dias de atraso. Sempre que o usuário pedir "faixas de atraso", utilize a coluna faixa_divida para agrupamento.

* 'Inadimplência' refere-se a faturas em aberto ('Aberto') com aging > 0 dias.

* Um cliente é considerado "Inadimplente" quando a coluna ind_inadimplente é igual a 'S'. Para valores financeiros de dívida, use sempre a soma da coluna val_divida.

* Use a tabela ibge_senso apenas para dados demográficos (população, área).

* Use a tabela cnae para descrever o setor de atuação das empresas.
```
### 06. Add JOINS
```
UPPER(`faturamento`.`cidade`) = UPPER(`ibge_senso`.`MUNNOMEX`)
```

### 07. Add uma MEASURE 'Inadimplência Crítica' 
```
faturamento.num_cliente,
SUM(faturamento.val_divida) as faturamento.total_divida_critica
WHERE faturamento.aging_divida > 90
GROUP BY faturamento.num_cliente
HAVING faturamento.total_divida_critica >= 2500
```
```
Synonyms: Alto Risco, Devedores Prioritários, Carteira Podre
Instructions: Essa métrica define o que é inadimplência crítica, e deve ser aplicada sempre quando algum usuário perguntar sobre análises referentes à clientes de alto risco, criticos 
```
### 08. Add SQL Queries com parameters
```
WITH Bairros_Com_Atividade AS (
    SELECT
        e.bairro,
        COUNT(*) as qtd_empresas
    FROM
        empresas_sp e
    JOIN
        cnae c ON e.cnae_principal = c.cod_cnae
    WHERE
        c.descricao ILIKE concat('%', :atividade_desc, '%')
    GROUP BY
        e.bairro
    HAVING COUNT(*) > 5  -- Filter for significance
)
SELECT
    f.bairro,
    b.qtd_empresas as densidade_do_setor,
    SUM(f.val_divida) / NULLIF(SUM(f.val_faturado), 0) as taxa_comprometimento_renda
FROM
    faturamento f
JOIN
    Bairros_Com_Atividade b ON f.bairro = b.bairro
GROUP BY
    f.bairro, b.qtd_empresas
ORDER BY
    densidade_do_setor DESC
```

### 09. Pergunte
```
Quais clientes apresentam alto risco
```
``` 
Existe correlação entre bairros com muitas lanchonetes e a inadimplência?
```

### 10. Perguntas para o Research Agent 
```
Estamos planejando uma campanha para novos clientes PJ. Com base no histórico de pagamentos e no capital social das empresas, quais bairros ou municípios apresentam o perfil de 'Baixo Risco' e 'Alto Potencial' (empresas de alto capital e pouca dívida)?
```
```
Descreva o dataset e importantes KPIs/categorias eu posso analizar.
```
```
Identifique outliers interessantes no conjunto de dados (e possíveis causas)?
```
### 11. Add Benchmarks
```
Qual é a quantidade total de agentes inadimplentes distintos?

SELECT
  COUNT(DISTINCT `num_cliente`) AS quantidade_agentes_inadimplentes
FROM
  `bbts`.`inadimplencia`.`faturamento`
WHERE
  `ind_inadimplente` = 'S'
```
```
Qual a quantidade de agentes inadimplentes?

SELECT
  COUNT(DISTINCT `num_cliente`) AS quantidade_agentes_inadimplentes
FROM
  `bbts`.`inadimplencia`.`faturamento`
WHERE
  `ind_inadimplente` = 'S'
```
```
Qual o valor total da dívida considerada inadimplência crítica (aging superior a 90 dias e dívida acima de R$ 2.500)?

SELECT
  SUM(val_divida) AS total_divida_critica
FROM
  bbts.inadimplencia.faturamento
WHERE
  aging_divida > 90
  AND val_divida >= 2500
```
```
Quais são os principais padrões de inadimplência observados no mercado de São Paulo?

SELECT
  faixa_divida,
  COUNT(*) AS quantidade_clientes,
  SUM(val_divida) AS soma_divida
FROM
  bbts.inadimplencia.faturamento
WHERE
  ind_inadimplente = 'S'
  AND aging_divida > 0
GROUP BY
  faixa_divida
ORDER BY
  soma_divida DESC
```
```
Qual a distribuição da população urbana e rural nos municípios de São Paulo conforme o IBGE?

SELECT
  MUNICIPIO,
  QDE_POP_URBANA,
  QDE_POP_RURAL
FROM
  bbts.inadimplencia.ibge_senso
WHERE
  UF = 'SP'
ORDER BY
  MUNICIPIO
```




