CREATE SCHEMA IF NOT EXISTS sfdc_stage;

-- Creating Objects 'account' and 'contact' with TEXT type for all columns 
-- No Relationships Implemented for Stage layer tables 
-- Added LOAD_TS Column with default value 
-- Partitioned by LastModifiedDate

DROP TABLE IF EXISTS sfdc_stage.account;
CREATE TABLE sfdc_stage.account(
	Id TEXT,
	IsDeleted TEXT,
	MasterRecordId TEXT,
	Name TEXT,
	Type TEXT,
	ParentId TEXT,
	BillingStreet TEXT,
	BillingCity TEXT,
	BillingState TEXT,
	BillingPostalCode TEXT,
	BillingCountry TEXT,
	BillingLatitude TEXT,
	BillingLongitude TEXT,
	BillingGeocodeAccuracy TEXT,
	BillingAddress TEXT,
	ShippingStreet TEXT,
	ShippingCity TEXT,
	ShippingState TEXT,
	ShippingPostalCode TEXT,
	ShippingCountry TEXT,
	ShippingLatitude TEXT,
	ShippingLongitude TEXT,
	ShippingGeocodeAccuracy TEXT,
	ShippingAddress TEXT,
	Phone TEXT,
	Fax TEXT,
	AccountNumber TEXT,
	Website TEXT,
	PhotoUrl TEXT,
	Sic TEXT,
	Industry TEXT,
	AnnualRevenue TEXT,
	NumberOfEmployees TEXT,
	Ownership TEXT,
	TickerSymbol TEXT,
	Description TEXT,
	Rating TEXT,
	Site TEXT,
	OwnerId TEXT,
	CreatedDate TEXT,
	CreatedById TEXT,
	LastModifiedDate TEXT,
	LastModifiedById TEXT,
	SystemModstamp TEXT,
	LastActivityDate TEXT,
	LastViewedDate TEXT,
	LastReferencedDate TEXT,
	Jigsaw TEXT,
	JigsawCompanyId TEXT,
	CleanStatus TEXT,
	AccountSource TEXT,
	DunsNumber TEXT,
	Tradestyle TEXT,
	NaicsCode TEXT,
	NaicsDesc TEXT,
	YearStarted TEXT,
	SicDesc TEXT,
	DandbCompanyId TEXT,
	CustomerPriority__c TEXT,
	SLA__c TEXT,
	Active__c TEXT,
	NumberofLocations__c TEXT,
	UpsellOpportunity__c TEXT,
	SLASerialNumber__c TEXT,
	SLAExpirationDate__c TEXT,
	LOAD_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
) PARTITION BY RANGE(LastModifiedDate);

DROP TABLE IF EXISTS sfdc_stage.contact;
CREATE TABLE sfdc_stage.contact (
	Id TEXT,
	IsDeleted TEXT,
	MasterRecordId TEXT,
	AccountId TEXT,
	LastName TEXT,
	FirstName TEXT,
	Salutation TEXT,
	Name TEXT,
	OtherStreet TEXT,
	OtherCity TEXT,
	OtherState TEXT,
	OtherPostalCode TEXT,
	OtherCountry TEXT,
	OtherLatitude TEXT,
	OtherLongitude TEXT,
	OtherGeocodeAccuracy TEXT,
	OtherAddress TEXT,
	MailingStreet TEXT,
	MailingCity TEXT,
	MailingState TEXT,
	MailingPostalCode TEXT,
	MailingCountry TEXT,
	MailingLatitude TEXT,
	MailingLongitude TEXT,
	MailingGeocodeAccuracy TEXT,
	MailingAddress TEXT,
	Phone TEXT,
	Fax TEXT,
	MobilePhone TEXT,
	HomePhone TEXT,
	OtherPhone TEXT,
	AssistantPhone TEXT,
	ReportsToId TEXT,
	Email TEXT,
	Title TEXT,
	Department TEXT,
	AssistantName TEXT,
	LeadSource TEXT,
	Birthdate TEXT,
	Description TEXT,
	OwnerId TEXT,
	CreatedDate TEXT,
	CreatedById TEXT,
	LastModifiedDate TEXT,
	LastModifiedById TEXT,
	SystemModstamp TEXT,
	LastActivityDate TEXT,
	LastCURequestDate TEXT,
	LastCUUpdateDate TEXT,
	LastViewedDate TEXT,
	LastReferencedDate TEXT,
	EmailBouncedReason TEXT,
	EmailBouncedDate TEXT,
	IsEmailBounced TEXT,
	PhotoUrl TEXT,
	Jigsaw TEXT,
	JigsawContactId TEXT,
	CleanStatus TEXT,
	IndividualId TEXT,
	IsPriorityRecord TEXT,
	ContactSource TEXT,
	Level__c TEXT,
	Languages__c TEXT,
	LOAD_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE(LastModifiedDate);

-- Verify tables, columns and datatypes
SELECT table_name, column_name, data_type 
FROM information_schema.columns
WHERE table_schema = 'sfdc_stage';

-- AFTER LOADING DATA 
-- verify If partitions are being built as expected  
SELECT 
    child.relname AS partition_name,
    parent.relname AS parent_table
FROM 
    pg_inherits 
JOIN 
    pg_class child ON child.oid = pg_inherits.inhrelid
JOIN 
    pg_class parent ON parent.oid = pg_inherits.inhparent
WHERE 
    parent.relname = 'account' 
    AND parent.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'sfdc_stage');
