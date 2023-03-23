CREATE TABLE client (
    client_id varchar(20) NOT NULL,
    first_name varchar(50) NOT NULL,
    last_name varchar(50) NOT NULL, coms_num varchar(50) NOT NULL,
    handlerhandler_id varchar(20) NOT NULL,
    alien_speciesspecies_id varchar(50) NOT NULL,
    PRIMARY KEY (client_id));

CREATE TABLE handler (
    handler_id varchar(20) NOT NULL,
    first_name varchar(50) NOT NULL,
    last_name varchar(50) NOT NULL,
    planetsplanet_id varchar(50) NOT NULL,
    PRIMARY KEY (handler_id));

CREATE TABLE job (
    job_id varchar(20) NOT NULL,
    target_first_name varchar(50) NOT NULL,
    target_last_name varchar(50) NULL,
    target_occupation varchar(200) NOT NULL,
    clientclient_id varchar(20) NOT NULL,
    mercenarymerc_id varchar(20) NOT NULL,
    alien_speciesspecies_id varchar(50) NOT NULL,
    planetsplanet_id varchar(50) NOT NULL,
    PRIMARY KEY (job_id));

CREATE TABLE mercenary (
    merc_id varchar(20) NOT NULL,
    alias varchar(50) NOT NULL,
    handlerhandler_id varchar(20) NOT NULL,
    alien_speciesspecies_id varchar(50) NOT NULL,
    PRIMARY KEY (merc_id));

CREATE TABLE planets (
    planet_id varchar(50) NOT NULL,
    planet_name varchar(50) NOT NULL UNIQUE,
    PRIMARY KEY (planet_id));

CREATE TABLE alien_species (
    species_id varchar(50) NOT NULL,
    species varchar(50) NOT NULL UNIQUE,
    PRIMARY KEY (species_id));

CREATE TABLE specialize (
    specialization_id varchar(50) NOT NULL,
    specialization varchar(50) NOT NULL UNIQUE,
    PRIMARY KEY (specialization_id));

CREATE TABLE [transaction] (
    transaction_id varchar(20) NOT NULL,
    pay_amount money NOT NULL,
    date_completed date NULL,
    jobjob_id varchar(20) NOT NULL,
    PRIMARY KEY (transaction_id));

CREATE TABLE mercenary_specialize (
    mercenarymerc_id varchar(20) NOT NULL,
    specializespecialization_id varchar(50) NOT NULL,
    PRIMARY KEY (mercenarymerc_id, specializespecialization_id));

ALTER TABLE job ADD CONSTRAINT mercenary_job FOREIGN KEY (mercenarymerc_id) REFERENCES mercenary (merc_id);

ALTER TABLE client ADD CONSTRAINT handler_client FOREIGN KEY (handlerhandler_id) REFERENCES handler (handler_id);

ALTER TABLE mercenary ADD CONSTRAINT handler_mercenary FOREIGN KEY (handlerhandler_id) REFERENCES handler (handler_id);

ALTER TABLE job ADD CONSTRAINT client_job FOREIGN KEY (clientclient_id) REFERENCES client (client_id);

ALTER TABLE mercenary ADD CONSTRAINT FKmercenary962839 FOREIGN KEY (alien_speciesspecies_id) REFERENCES alien_species (species_id);

ALTER TABLE job ADD CONSTRAINT FKjob142778 FOREIGN KEY (alien_speciesspecies_id) REFERENCES alien_species (species_id);

ALTER TABLE client ADD CONSTRAINT FKclient9613 FOREIGN KEY (alien_speciesspecies_id) REFERENCES alien_species (species_id);

ALTER TABLE handler ADD CONSTRAINT FKhandler213046 FOREIGN KEY (planetsplanet_id) REFERENCES planets (planet_id);

ALTER TABLE job ADD CONSTRAINT FKjob911735 FOREIGN KEY (planetsplanet_id) REFERENCES planets (planet_id);

ALTER TABLE [transaction] ADD CONSTRAINT FKtransactio332899 FOREIGN KEY (jobjob_id) REFERENCES job (job_id);

ALTER TABLE mercenary_specialize ADD CONSTRAINT FKmercenary_396628 FOREIGN KEY (mercenarymerc_id) REFERENCES mercenary (merc_id);

ALTER TABLE mercenary_specialize ADD CONSTRAINT FKmercenary_689813 FOREIGN KEY (specializespecialization_id) REFERENCES specialize (specialization_id);
