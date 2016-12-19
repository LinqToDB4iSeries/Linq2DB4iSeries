DROP TABLE LINQ2DB/Doctor
GO
DROP TABLE LINQ2DB/Patient
GO
DROP TABLE LINQ2DB/Person
GO
CREATE TABLE LINQ2DB/Person( 
	PersonID   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL,
	FirstName  VARCHAR(50) NOT NULL,
	LastName   VARCHAR(50) NOT NULL,
	MiddleName VARCHAR(50),
	Gender     CHAR(1)     NOT NULL
)
GO
INSERT INTO LINQ2DB/Person (FirstName, LastName, Gender) VALUES ('John',   'Pupkin',    'M')
GO
INSERT INTO LINQ2DB/Person (FirstName, LastName, Gender) VALUES ('Tester', 'Testerson', 'M')
GO
INSERT INTO LINQ2DB/Person (FirstName, LastName, Gender) VALUES ( 'Miss', 'Scarlet', 'F')
GO
INSERT INTO LINQ2DB/Person (FirstName, LastName, Gender) VALUES ('Rev', 'Green', 'M')
GO
INSERT INTO LINQ2DB/Person (FirstName, LastName, Gender) VALUES ('Col', 'Mustard', 'M')
GO
INSERT INTO LINQ2DB/Person (FirstName, LastName, Gender) VALUES ('Mrs','Peacock', 'F')
GO
INSERT INTO LINQ2DB/Person (FirstName, LastName, Gender) VALUES ('Prof', 'Plum','M')
GO
INSERT INTO LINQ2DB/Person (FirstName, LastName, Gender) VALUES ('Mrs', 'White', 'F')
GO

-- Doctor Table Extension

CREATE TABLE LINQ2DB/Doctor(
	PersonID INTEGER     NOT NULL,
	Taxonomy VARCHAR(50) NOT NULL,
	FOREIGN KEY FK_Doctor_Person(PersonID) REFERENCES LINQ2DB/Person
)
GO
INSERT INTO LINQ2DB/Doctor (PersonID, Taxonomy) VALUES (1, 'Psychiatry')
GO

DROP TABLE LINQ2DB/MasterTable
GO
DROP TABLE LINQ2DB/SlaveTable
GO
CREATE TABLE LINQ2DB/MasterTable(
	ID1 INTEGER NOT NULL,
	ID2 INTEGER NOT NULL,
	PRIMARY KEY (ID1,ID2)
)
GO
CREATE TABLE LINQ2DB/SlaveTable(
	ID1    INTEGER NOT NULL,
	"ID 2222222222222222222222  22" INTEGER NOT NULL,
	"ID 2222222222222222"           INTEGER NOT NULL,
	FOREIGN KEY FK_SlaveTable_MasterTable ("ID 2222222222222222222222  22", ID1)
	REFERENCES LINQ2DB/MasterTable
)
GO

CREATE TABLE LINQ2DB/Patient(
	PersonID  INTEGER      NOT NULL,
	Diagnosis VARCHAR(256) NOT NULL
)
GO
INSERT INTO LINQ2DB/Patient(PersonID, Diagnosis) VALUES (2, 'Hallucination with Paranoid Bugs'' Delirium of Persecution')
GO
DROP TABLE LINQ2DB/Parent
GO
DROP TABLE LINQ2DB/Child
GO
DROP TABLE LINQ2DB/GrandChild
GO
CREATE TABLE LINQ2DB/Parent      (ParentID int, Value1 int)
GO
CREATE TABLE LINQ2DB/Child       (ParentID int, ChildID int)
GO
CREATE TABLE LINQ2DB/GrandChild  (ParentID int, ChildID int, GrandChildID int)
GO
DROP TABLE LINQ2DB/LinqDataTypes
GO
CREATE TABLE LINQ2DB/LinqDataTypes(
	ID             int,
	MoneyValue     decimal(10,4),
	DateTimeValue  timestamp,
	DateTimeValue2 timestamp  Default NULL,
	BoolValue      smallint,
	GuidValue      char(16) for bit DATA,
	BinaryValue    blob(5000) Default NULL,
	SmallIntValue  smallint,
	IntValue       int        Default NULL,
	BigIntValue    bigint     Default NULL
)
GO
DROP TABLE LINQ2DB/TestIdentity
GO
CREATE TABLE LINQ2DB/TestIdentity (
	ID   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL
)
GO
DROP TABLE LINQ2DB/AllTypes
GO
CREATE TABLE LINQ2DB/AllTypes(
	  ID INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL

	, bigintDataType           bigint                  Default NULL
	, binaryDataType           binary(20)              Default NULL
	, blobDataType             blob                    Default NULL
	, charDataType             char(1)                 Default NULL
	, CharForBitDataType       char(5) for bit data    Default NULL
	, clobDataType             clob                    Default NULL
	, dataLinkDataType         dataLink                Default NULL
	, dateDataType             date                    Default NULL
	, dbclobDataType           dbclob(100)             Default NULL
	, decfloat16DataType       decfloat(16)                Default NULL
	, decfloat34DataType       decfloat(34)               Default NULL
	, decimalDataType          decimal(30)             Default NULL
	, doubleDataType           double                  Default NULL
	, graphicDataType          graphic(10)             Default NULL
    , intDataType              int                     Default NULL
	, numericDataType          numeric                 Default NULL
	, realDataType             real                    Default NULL
	, rowIdDataType            rowId                              
	, smallintDataType         smallint                Default NULL
	, timeDataType             time                    Default NULL
	, timestampDataType        timestamp               Default NULL
	, varbinaryDataType        varbinary(20)           Default NULL
	, varcharDataType          varchar(20)             Default NULL
	, varCharForBitDataType    varchar(5) for bit data Default NULL
	, varGraphicDataType       vargraphic(10)          Default NULL
--, xmlDataType              xml(20)                 Default NULL
)
GO

INSERT INTO LINQ2DB/AllTypes (bigintDataType) VALUES (NULL)

GO
INSERT INTO LINQ2DB/AllTypes(
	  bigintDataType           
	, binaryDataType           
	, blobDataType             
	, charDataType             
	, CharForBitDataType       
	, clobDataType             
	, dataLinkDataType         
	, dateDataType             
	, dbclobDataType           
	, decfloat16DataType       
	, decfloat34DataType       
	, decimalDataType          
	, doubleDataType           
	, graphicDataType          
	, intDataType              
	, numericDataType          
	, realDataType             
	, rowIdDataType            
	, smallintDataType         
	, timeDataType             
	, timestampDataType        
	, varbinaryDataType        
	, varcharDataType          
	, varCharForBitDataType    
	, varGraphicDataType       
--	, xmlDataType              
) VALUES (
	  1000000                    --bigIntDataType         
	, Cast('123' as binary)      --binaryDataType
	, Cast('234' as blob)        --blobDataType             
	, 'Y'                        --charDataType             
	, '123'                      --CharForBitDataType       
	, Cast('567' as clob)        --clobDataType             
	, DEFAULT                    --dataLinkDataType         
	, '2012-12-12'               --dateDataType             
	, Cast('890' as dbclob)      --dbclobDataType           
	, 888.456                    --decfloat16DataType       
	, 777.987                    --decfloat34DataType       
	, 666.987                    --decimalDataType          
	, 555.987                    --doubleDataType           
	, DEFAULT --Cast('graphic' as graphic) --graphicDataType          gets error when casting the data
  , 444444                     --intDataType              
	, 333.987                    --numericDataType          
	, 222.987                    --realDataType             
	, DEFAULT                    --rowIdDataType            
	, 100                        --smallintDataType         
	, '12:12:12'               --timeDataType             
	, '2012-12-12 12:12:12'          --timestampDataType        
	, Cast('456' as binary)      --varbinaryDataType        
	, 'var-char'                 --varcharDataType          
	, 'vcfb'                     --varCharForBitDataType    
	, DEFAULT                    --varGraphicDataType
  
--	, '<root><element strattr="strvalue" intattr="12345"/></root>' --xmlDataType  
)
GO

DROP VIEW LINQ2DB/PersonView
GO
CREATE VIEW LINQ2DB/PersonView
AS
SELECT * FROM LINQ2DB/Person
GO
DROP Procedure LINQ2DB/Person_SelectByKey
GO
CREATE Procedure LINQ2DB/Person_SelectByKey(in ID integer)
RESULT SETS 1
LANGUAGE SQL
BEGIN
	DECLARE C1 CURSOR FOR
		SELECT * FROM LINQ2DB/Person WHERE PersonID = ID;

	OPEN C1;
END
GO
