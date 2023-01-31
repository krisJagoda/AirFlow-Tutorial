-- script to create the cat table

CREATE TABLE IF NOT EXISTS cat
(
    cat_id     int PRIMARY KEY,
    cat_name   varchar(255) NOT NULL,
    birth_date date         NOT NULL,
    age        int          NOT NULL,
    carer_name varchar(255) NOT NULL
);
