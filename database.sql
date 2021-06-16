
create table if not exists rates (
  refdate TEXT,
  name TEXT,
  value REAL,
  source TEXT
);

CREATE UNIQUE INDEX rates_index ON rates(refdate, name);

create table if not exists indexes (
  refdate TEXT,
  name TEXT,
  value REAL,
  source TEXT
);

CREATE UNIQUE INDEX indexes_index ON indexes(refdate, name);

