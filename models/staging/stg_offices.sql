{{config(materialized='table',transient= false )}}

select Officeid, OfficeAddress as Address, OfficePostalCode as PostalCode, OfficeCity as City,
OfficeStateProvince as StateProvince, OfficePhone as Phone, OfficeFax as Fax,
OfficeCountry as Country 
 from {{source('raw_qwt','raw_offices')}} order by 1