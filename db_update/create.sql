create table sf_requests (
        id                      SERIAL PRIMARY KEY NOT NULL,
        status                  VARCHAR(10) DEFAULT NULL,
        description             text DEFAULT NULL,
        service_code            text DEFAULT NULL,
        service_name            text DEFAULT NULL,
        service_request_id      integer DEFAULT NULL,
        requested_datetime      timestamp DEFAULT NULL,
        expected_datetime       timestamp DEFAULT NULL,
        updated_datetime        timestamp DEFAULT NULL,
        address                 text DEFAULT NULL,
        zipcode                 integer DEFAULT NULL,
        lon                     double precision DEFAULT NULL,
        lat                     double precision DEFAULT NULL
);

ALTER TABLE sf_requests ADD COLUMN neighborhood text DEFAULT NULL;
ALTER TABLE sf_requests ADD COLUMN category text DEFAULT NULL;

CREATE INDEX requested_day ON sf_requests ( DATE(requested_datetime) );
CREATE INDEX updated_day ON sf_requests ( DATE(updated_datetime) );
CREATE INDEX neighborhood ON sf_requests ( neighborhood );
CREATE INDEX request_status ON sf_requests ( status );
