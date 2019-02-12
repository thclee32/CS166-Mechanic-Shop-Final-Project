CREATE LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION return_val() RETURNS trigger AS
$$
BEGIN 
NEW.custID = nextval('id');
RETURN NEW;
END;
$$
LANGUAGE plpgsql VOLATILE;


CREATE TRIGGER set_customer_id
BEFORE INSERT 
ON Customer
FOR EACH ROW
EXECUTE PROCEDURE return_val();
