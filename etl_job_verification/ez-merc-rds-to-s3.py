import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


def directJDBCSource(
    glueContext,
    connectionName,
    connectionType,
    database,
    table,
    redshiftTmpDir,
    transformation_ctx,
) -> DynamicFrame:
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": table,
        "connectionName": connectionName,
    }

    if redshiftTmpDir:
        connection_options["redshiftTmpDir"] = redshiftTmpDir

    return glueContext.create_dynamic_frame.from_options(
        connection_type=connectionType,
        connection_options=connection_options,
        transformation_ctx=transformation_ctx,
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node trans_source
trans_source_node1679416816191 = directJDBCSource(
    glueContext,
    connectionName="ez-merc-connection-2",
    connectionType="sqlserver",
    database="ez-merc-ig-db.cqznmpskxrfs.us-west-2.rds.amazonaws.com:1433;databaseName=EZMercIGdb",
    table="dbo.transaction",
    redshiftTmpDir="",
    transformation_ctx="trans_source_node1679416816191",
)

# Script generated for node planet_source
planet_source_node1679416633872 = directJDBCSource(
    glueContext,
    connectionName="ez-merc-connection-2",
    connectionType="sqlserver",
    database="ez-merc-ig-db.cqznmpskxrfs.us-west-2.rds.amazonaws.com:1433;databaseName=EZMercIGdb",
    table="dbo.planets",
    redshiftTmpDir="",
    transformation_ctx="planet_source_node1679416633872",
)

# Script generated for node species_source
species_source_node1 = directJDBCSource(
    glueContext,
    connectionName="ez-merc-connection-2",
    connectionType="sqlserver",
    database="ez-merc-ig-db.cqznmpskxrfs.us-west-2.rds.amazonaws.com:1433;databaseName=EZMercIGdb",
    table="dbo.alien_species",
    redshiftTmpDir="",
    transformation_ctx="species_source_node1",
)

# Script generated for node handler_source
handler_source_node1679416296615 = directJDBCSource(
    glueContext,
    connectionName="ez-merc-connection-2",
    connectionType="sqlserver",
    database="ez-merc-ig-db.cqznmpskxrfs.us-west-2.rds.amazonaws.com:1433;databaseName=EZMercIGdb",
    table="dbo.handler",
    redshiftTmpDir="",
    transformation_ctx="handler_source_node1679416296615",
)

# Script generated for node job_source
job_source_node1679416369538 = directJDBCSource(
    glueContext,
    connectionName="ez-merc-connection-2",
    connectionType="sqlserver",
    database="ez-merc-ig-db.cqznmpskxrfs.us-west-2.rds.amazonaws.com:1433;databaseName=EZMercIGdb",
    table="dbo.job",
    redshiftTmpDir="",
    transformation_ctx="job_source_node1679416369538",
)

# Script generated for node merc_source
merc_source_node1679416545600 = directJDBCSource(
    glueContext,
    connectionName="ez-merc-connection-2",
    connectionType="sqlserver",
    database="ez-merc-ig-db.cqznmpskxrfs.us-west-2.rds.amazonaws.com:1433;databaseName=EZMercIGdb",
    table="dbo.mercenary",
    redshiftTmpDir="",
    transformation_ctx="merc_source_node1679416545600",
)

# Script generated for node spec_merc_source
spec_merc_source_node1679416442506 = directJDBCSource(
    glueContext,
    connectionName="ez-merc-connection-2",
    connectionType="sqlserver",
    database="ez-merc-ig-db.cqznmpskxrfs.us-west-2.rds.amazonaws.com:1433;databaseName=EZMercIGdb",
    table="dbo.specialize_Mercenary",
    redshiftTmpDir="",
    transformation_ctx="spec_merc_source_node1679416442506",
)

# Script generated for node client_source
client_source_node1679416211537 = directJDBCSource(
    glueContext,
    connectionName="ez-merc-connection-2",
    connectionType="sqlserver",
    database="ez-merc-ig-db.cqznmpskxrfs.us-west-2.rds.amazonaws.com:1433;databaseName=EZMercIGdb",
    table="dbo.client",
    redshiftTmpDir="",
    transformation_ctx="client_source_node1679416211537",
)

# Script generated for node spec_source
spec_source_node1679416734655 = directJDBCSource(
    glueContext,
    connectionName="ez-merc-connection-2",
    connectionType="sqlserver",
    database="ez-merc-ig-db.cqznmpskxrfs.us-west-2.rds.amazonaws.com:1433;databaseName=EZMercIGdb",
    table="dbo.specialize",
    redshiftTmpDir="",
    transformation_ctx="spec_source_node1679416734655",
)

# Script generated for node trans_bucket
trans_bucket_node1679416854047 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-raw/transaction/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="trans_bucket_node1679416854047",
)
trans_bucket_node1679416854047.setCatalogInfo(
    catalogDatabase="ez-merc-db", catalogTableName="trans"
)
trans_bucket_node1679416854047.setFormat("glueparquet")
trans_bucket_node1679416854047.writeFrame(trans_source_node1679416816191)
# Script generated for node planet_bucket
planet_bucket_node1679416636015 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-raw/planet/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="planet_bucket_node1679416636015",
)
planet_bucket_node1679416636015.setCatalogInfo(
    catalogDatabase="ez-merc-db", catalogTableName="planet"
)
planet_bucket_node1679416636015.setFormat("glueparquet")
planet_bucket_node1679416636015.writeFrame(planet_source_node1679416633872)
# Script generated for node species_bucket
species_bucket_node3 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-raw/alien_species/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="species_bucket_node3",
)
species_bucket_node3.setCatalogInfo(
    catalogDatabase="ez-merc-db", catalogTableName="alien_species"
)
species_bucket_node3.setFormat("glueparquet")
species_bucket_node3.writeFrame(species_source_node1)
# Script generated for node handler_bucket
handler_bucket_node1679416330684 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-raw/handler/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="handler_bucket_node1679416330684",
)
handler_bucket_node1679416330684.setCatalogInfo(
    catalogDatabase="ez-merc-db", catalogTableName="handler"
)
handler_bucket_node1679416330684.setFormat("glueparquet")
handler_bucket_node1679416330684.writeFrame(handler_source_node1679416296615)
# Script generated for node job_bucket
job_bucket_node1679416400394 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-raw/job/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="job_bucket_node1679416400394",
)
job_bucket_node1679416400394.setCatalogInfo(
    catalogDatabase="ez-merc-db", catalogTableName="job"
)
job_bucket_node1679416400394.setFormat("glueparquet")
job_bucket_node1679416400394.writeFrame(job_source_node1679416369538)
# Script generated for node merc_bucket
merc_bucket_node1679416578593 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-raw/merc/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="merc_bucket_node1679416578593",
)
merc_bucket_node1679416578593.setCatalogInfo(
    catalogDatabase="ez-merc-db", catalogTableName="merc"
)
merc_bucket_node1679416578593.setFormat("glueparquet")
merc_bucket_node1679416578593.writeFrame(merc_source_node1679416545600)
# Script generated for node spec_merc_bucket
spec_merc_bucket_node1679416479546 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-raw/merc-specialize/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="spec_merc_bucket_node1679416479546",
)
spec_merc_bucket_node1679416479546.setCatalogInfo(
    catalogDatabase="ez-merc-db", catalogTableName="spec_merc"
)
spec_merc_bucket_node1679416479546.setFormat("glueparquet")
spec_merc_bucket_node1679416479546.writeFrame(spec_merc_source_node1679416442506)
# Script generated for node client_bucket
client_bucket_node1679416213620 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-raw/client/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="client_bucket_node1679416213620",
)
client_bucket_node1679416213620.setCatalogInfo(
    catalogDatabase="ez-merc-db", catalogTableName="client"
)
client_bucket_node1679416213620.setFormat("glueparquet")
client_bucket_node1679416213620.writeFrame(client_source_node1679416211537)
# Script generated for node spec_bucket
spec_bucket_node1679416777568 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-raw/specialize/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="spec_bucket_node1679416777568",
)
spec_bucket_node1679416777568.setCatalogInfo(
    catalogDatabase="ez-merc-db", catalogTableName="spec"
)
spec_bucket_node1679416777568.setFormat("glueparquet")
spec_bucket_node1679416777568.writeFrame(spec_source_node1679416734655)
job.commit()
