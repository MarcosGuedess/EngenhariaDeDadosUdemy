import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Clientes
Clientes_node1668713478463 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="clientes_csv",
    transformation_ctx="Clientes_node1668713478463",
)

# Script generated for node Vendedores
Vendedores_node1668713904418 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="vendedores_csv",
    transformation_ctx="Vendedores_node1668713904418",
)

# Script generated for node Vendas
Vendas_node1668712946998 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="vendas_csv",
    transformation_ctx="Vendas_node1668712946998",
)

# Script generated for node Produto
Produto_node1668713666853 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="produtos_csv",
    transformation_ctx="Produto_node1668713666853",
)

# Script generated for node itensVendas
itensVendas_node1668713205954 = glueContext.create_dynamic_frame.from_catalog(
    database="vendas",
    table_name="itensvenda_csv",
    transformation_ctx="itensVendas_node1668713205954",
)

# Script generated for node VendasMapping
VendasMapping_node1668713091074 = ApplyMapping.apply(
    frame=Vendas_node1668712946998,
    mappings=[
        ("idvenda", "long", "idvenda", "long"),
        ("idvendedor", "long", "idvendedor_vendas", "long"),
        ("idcliente", "long", "idcliente_vendas", "long"),
        ("data", "string", "data", "string"),
        ("total", "double", "total", "double"),
    ],
    transformation_ctx="VendasMapping_node1668713091074",
)

# Script generated for node ItensVendas_Mapping
ItensVendas_Mapping_node1668713253988 = ApplyMapping.apply(
    frame=itensVendas_node1668713205954,
    mappings=[
        ("idproduto", "long", "idproduto_itensvendas", "long"),
        ("idvenda", "long", "idvenda_itensasvenda", "long"),
        ("quantidade", "long", "quantidade", "long"),
        ("valorunitario", "double", "valorunitario", "double"),
        ("valortotal", "double", "valortotal", "double"),
        ("desconto", "double", "desconto", "double"),
    ],
    transformation_ctx="ItensVendas_Mapping_node1668713253988",
)

# Script generated for node JoinVendas_ItensVendas
JoinVendas_ItensVendas_node1668713375146 = Join.apply(
    frame1=VendasMapping_node1668713091074,
    frame2=ItensVendas_Mapping_node1668713253988,
    keys1=["idvenda"],
    keys2=["idvenda_itensasvenda"],
    transformation_ctx="JoinVendas_ItensVendas_node1668713375146",
)

# Script generated for node JoinClientes
JoinClientes_node1668713520019 = Join.apply(
    frame1=Clientes_node1668713478463,
    frame2=JoinVendas_ItensVendas_node1668713375146,
    keys1=["idcliente"],
    keys2=["idcliente_vendas"],
    transformation_ctx="JoinClientes_node1668713520019",
)

# Script generated for node JoinProduto
JoinProduto_node1668713705822 = Join.apply(
    frame1=Produto_node1668713666853,
    frame2=JoinClientes_node1668713520019,
    keys1=["idproduto"],
    keys2=["idproduto_itensvendas"],
    transformation_ctx="JoinProduto_node1668713705822",
)

# Script generated for node JoinVendedores
JoinVendedores_node1668713933957 = Join.apply(
    frame1=Vendedores_node1668713904418,
    frame2=JoinProduto_node1668713705822,
    keys1=["idvendedor"],
    keys2=["idvendedor_vendas"],
    transformation_ctx="JoinVendedores_node1668713933957",
)

# Script generated for node ColunasFinais
ColunasFinais_node1668714207204 = ApplyMapping.apply(
    frame=JoinVendedores_node1668713933957,
    mappings=[
        ("nome", "string", "nome", "string"),
        ("produto", "string", "produto", "string"),
        ("preco", "double", "preco", "double"),
        ("cliente", "string", "cliente", "string"),
        ("estado", "string", "estado", "string"),
        ("sexo", "string", "sexo", "string"),
        ("status", "string", "status", "string"),
        ("data", "string", "data", "string"),
        ("total", "double", "total", "double"),
        ("quantidade", "long", "quantidade", "long"),
        ("valorunitario", "double", "valorunitario", "double"),
        ("valortotal", "double", "valortotal", "double"),
        ("desconto", "double", "desconto", "double"),
    ],
    transformation_ctx="ColunasFinais_node1668714207204",
)

# Script generated for node DataLake
DataLake_node1668714321228 = glueContext.write_dynamic_frame.from_options(
    frame=ColunasFinais_node1668714207204,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://glue-datalake123/datalake/",
        "partitionKeys": ["status"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="DataLake_node1668714321228",
)

job.commit()
