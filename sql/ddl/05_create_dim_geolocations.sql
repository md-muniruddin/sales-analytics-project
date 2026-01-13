CREATE TABLE dim_geolocations(
geolocation_zip_code_prefix INT NOT NULL,
geolocation_lat DECIMAL(11,8)  NOT NULL,
geolocation_lng DECIMAL(11,8)  NOT NULL,
geolocation_city VARCHAR(50) NOT NULL,
geolocation_state VARCHAR(50) NOT NULL,
geolocation_city_en VARCHAR(50) NOT NULL
)