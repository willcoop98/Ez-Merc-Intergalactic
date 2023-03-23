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

# Script generated for node client_raw
client_raw_node1679417785244 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://ez-merc-intergalactic/ez-merc-raw/client/"],
        "recurse": True,
    },
    transformation_ctx="client_raw_node1679417785244",
)

# Script generated for node planet_raw
planet_raw_node1679418280693 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://ez-merc-intergalactic/ez-merc-raw/planet/"],
        "recurse": True,
    },
    transformation_ctx="planet_raw_node1679418280693",
)

# Script generated for node merc_raw
merc_raw_node1679418166252 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://ez-merc-intergalactic/ez-merc-raw/merc/"],
        "recurse": True,
    },
    transformation_ctx="merc_raw_node1679418166252",
)

# Script generated for node species_raw
species_raw_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://ez-merc-intergalactic/ez-merc-raw/alien_species/"],
        "recurse": True,
    },
    transformation_ctx="species_raw_node1",
)

# Script generated for node trans_raw
trans_raw_node1679419774640 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://ez-merc-intergalactic/ez-merc-raw/transaction/"],
        "recurse": True,
    },
    transformation_ctx="trans_raw_node1679419774640",
)

# Script generated for node merc_spec_raw
merc_spec_raw_node1679418068628 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://ez-merc-intergalactic/ez-merc-raw/merc-specialize/"],
        "recurse": True,
    },
    transformation_ctx="merc_spec_raw_node1679418068628",
)

# Script generated for node spec_raw
spec_raw_node1679419687959 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://ez-merc-intergalactic/ez-merc-raw/specialize/"],
        "recurse": True,
    },
    transformation_ctx="spec_raw_node1679419687959",
)

# Script generated for node handler_raw
handler_raw_node1679417873060 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://ez-merc-intergalactic/ez-merc-raw/handler/"],
        "recurse": True,
    },
    transformation_ctx="handler_raw_node1679417873060",
)

# Script generated for node job_raw
job_raw_node1679417966100 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://ez-merc-intergalactic/ez-merc-raw/job/"],
        "recurse": True,
    },
    transformation_ctx="job_raw_node1679417966100",
)

# Script generated for node client_edit
client_edit_node1679417790043 = ApplyMapping.apply(
    frame=client_raw_node1679417785244,
    mappings=[
        ("client_id", "string", "client_id", "string"),
        ("first_name", "string", "first_name", "string"),
        ("last_name", "string", "last_name", "string"),
        ("coms_num", "string", "coms_num", "string"),
        ("handlerhandler_id", "string", "handlerhandler_id", "string"),
        ("alien_speciesspecies_id", "string", "alien_speciesspecies_id", "string"),
    ],
    transformation_ctx="client_edit_node1679417790043",
)

# Script generated for node planet_edit
planet_edit_node1679418286013 = ApplyMapping.apply(
    frame=planet_raw_node1679418280693,
    mappings=[
        ("planet_id", "string", "planet_id", "string"),
        ("planet_name", "string", "planet_name", "string"),
    ],
    transformation_ctx="planet_edit_node1679418286013",
)

# Script generated for node merc_edit
merc_edit_node1679418168013 = ApplyMapping.apply(
    frame=merc_raw_node1679418166252,
    mappings=[
        ("merc_id", "string", "merc_id", "string"),
        ("alias", "string", "alias", "string"),
        ("handlerhandler_id", "string", "handlerhandler_id", "string"),
        ("alien_speciesspecies_id", "string", "alien_speciesspecies_id", "string"),
    ],
    transformation_ctx="merc_edit_node1679418168013",
)

# Script generated for node species_edit
species_edit_node2 = ApplyMapping.apply(
    frame=species_raw_node1,
    mappings=[
        ("species_id", "string", "species_id", "string"),
        ("species", "string", "species", "string"),
    ],
    transformation_ctx="species_edit_node2",
)

# Script generated for node trans_edit
trans_edit_node1679419776400 = ApplyMapping.apply(
    frame=trans_raw_node1679419774640,
    mappings=[
        ("transaction_id", "string", "transaction_id", "string"),
        ("pay_amount", "decimal", "pay_amount", "decimal"),
        ("date_completed", "date", "date_completed", "date"),
        ("clientclient_id", "string", "clientclient_id", "string"),
        ("jobjob_id", "string", "jobjob_id", "string"),
    ],
    transformation_ctx="trans_edit_node1679419776400",
)

# Script generated for node merc_spec_edit
merc_spec_edit_node1679418070444 = ApplyMapping.apply(
    frame=merc_spec_raw_node1679418068628,
    mappings=[
        (
            "specializespecialization_id",
            "string",
            "specializespecialization_id",
            "string",
        ),
        ("mercenarymerc_id", "string", "mercenarymerc_id", "string"),
    ],
    transformation_ctx="merc_spec_edit_node1679418070444",
)

# Script generated for node spec_edit
spec_edit_node1679419690224 = ApplyMapping.apply(
    frame=spec_raw_node1679419687959,
    mappings=[
        ("specialization_id", "string", "specialization_id", "string"),
        ("specialization", "string", "specialization", "string"),
    ],
    transformation_ctx="spec_edit_node1679419690224",
)

# Script generated for node handler_edit
handler_edit_node1679417876252 = ApplyMapping.apply(
    frame=handler_raw_node1679417873060,
    mappings=[
        ("handler_id", "string", "handler_id", "string"),
        ("first_name", "string", "first_name", "string"),
        ("last_name", "string", "last_name", "string"),
        ("planetsplanet_id", "string", "planetsplanet_id", "string"),
    ],
    transformation_ctx="handler_edit_node1679417876252",
)

# Script generated for node job_edit
job_edit_node1679417971324 = ApplyMapping.apply(
    frame=job_raw_node1679417966100,
    mappings=[
        ("job_id", "string", "job_id", "string"),
        ("target_first_name", "string", "target_first_name", "string"),
        ("target_last_name", "string", "target_last_name", "string"),
        ("target_occupation", "string", "target_occupation", "string"),
        ("clientclient_id", "string", "clientclient_id", "string"),
        ("mercenarymerc_id", "string", "mercenarymerc_id", "string"),
        ("alien_speciesspecies_id", "string", "alien_speciesspecies_id", "string"),
        ("planetsplanet_id", "string", "planetsplanet_id", "string"),
    ],
    transformation_ctx="job_edit_node1679417971324",
)

# Script generated for node client_clean
client_clean_node1679417795877 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-data/client/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="client_clean_node1679417795877",
)
client_clean_node1679417795877.setCatalogInfo(
    catalogDatabase="ez-merc-clean", catalogTableName="client_clean"
)
client_clean_node1679417795877.setFormat("glueparquet")
client_clean_node1679417795877.writeFrame(client_edit_node1679417790043)
# Script generated for node planet_clean
planet_clean_node1679418291709 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-data/planet/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="planet_clean_node1679418291709",
)
planet_clean_node1679418291709.setCatalogInfo(
    catalogDatabase="ez-merc-clean", catalogTableName="planet_clean"
)
planet_clean_node1679418291709.setFormat("glueparquet")
planet_clean_node1679418291709.writeFrame(planet_edit_node1679418286013)
# Script generated for node merc_clean
merc_clean_node1679418169964 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-data/merc/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="merc_clean_node1679418169964",
)
merc_clean_node1679418169964.setCatalogInfo(
    catalogDatabase="ez-merc-clean", catalogTableName="merc_clean"
)
merc_clean_node1679418169964.setFormat("glueparquet")
merc_clean_node1679418169964.writeFrame(merc_edit_node1679418168013)
# Script generated for node species_clean
species_clean_node3 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-data/alien_species/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="species_clean_node3",
)
species_clean_node3.setCatalogInfo(
    catalogDatabase="ez-merc-clean", catalogTableName="species_clean"
)
species_clean_node3.setFormat("glueparquet")
species_clean_node3.writeFrame(species_edit_node2)
# Script generated for node trans_clean
trans_clean_node1679419778080 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-data/transaction/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="trans_clean_node1679419778080",
)
trans_clean_node1679419778080.setCatalogInfo(
    catalogDatabase="ez-merc-clean", catalogTableName="trans_clean"
)
trans_clean_node1679419778080.setFormat("glueparquet")
trans_clean_node1679419778080.writeFrame(trans_edit_node1679419776400)
# Script generated for node merc_spec_clean
merc_spec_clean_node1679418072718 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-data/merc_specialize/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="merc_spec_clean_node1679418072718",
)
merc_spec_clean_node1679418072718.setCatalogInfo(
    catalogDatabase="ez-merc-clean", catalogTableName="merc_spec_clean"
)
merc_spec_clean_node1679418072718.setFormat("glueparquet")
merc_spec_clean_node1679418072718.writeFrame(merc_spec_edit_node1679418070444)
# Script generated for node spec_clean
spec_clean_node1679419692297 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-data/specialize/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="spec_clean_node1679419692297",
)
spec_clean_node1679419692297.setCatalogInfo(
    catalogDatabase="ez-merc-clean", catalogTableName="spec_clean"
)
spec_clean_node1679419692297.setFormat("glueparquet")
spec_clean_node1679419692297.writeFrame(spec_edit_node1679419690224)
# Script generated for node handler_clean
handler_clean_node1679417879836 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-data/handler/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="handler_clean_node1679417879836",
)
handler_clean_node1679417879836.setCatalogInfo(
    catalogDatabase="ez-merc-clean", catalogTableName="handler_clean"
)
handler_clean_node1679417879836.setFormat("glueparquet")
handler_clean_node1679417879836.writeFrame(handler_edit_node1679417876252)
# Script generated for node job_clean
job_clean_node1679417973190 = glueContext.getSink(
    path="s3://ez-merc-intergalactic/ez-merc-data/job/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="job_clean_node1679417973190",
)
job_clean_node1679417973190.setCatalogInfo(
    catalogDatabase="ez-merc-clean", catalogTableName="job_clean"
)
job_clean_node1679417973190.setFormat("glueparquet")
job_clean_node1679417973190.writeFrame(job_edit_node1679417971324)
job.commit()
