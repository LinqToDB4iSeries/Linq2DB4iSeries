DROP TABLE Doctor
GO
DROP TABLE Patient
GO
DROP TABLE Person
GO
DROP TABLE InheritanceParent
GO

CREATE TABLE InheritanceParent
(
	InheritanceParentId INTEGER       PRIMARY KEY NOT NULL,
	TypeDiscriminator   INTEGER                       Default NULL,
	Name                NVARCHAR(50)                   Default NULL
)
GO

DROP TABLE InheritanceChild
GO

CREATE TABLE InheritanceChild
(
	InheritanceChildId  INTEGER      PRIMARY KEY NOT NULL,
	InheritanceParentId INTEGER                  NOT NULL,
	TypeDiscriminator   INTEGER                      Default NULL,
	Name                NVARCHAR(50)                  Default NULL
)
GO

CREATE TABLE Person( 
	PersonID   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL,
	FirstName  NVARCHAR(50) NOT NULL,
	LastName   NVARCHAR(50) NOT NULL,
	MiddleName NVARCHAR(50) ,
	Gender     NCHAR(1)     NOT NULL
)
GO

INSERT INTO Person (FirstName, LastName, Gender) VALUES ('John',   'Pupkin',    'M')
GO
INSERT INTO Person (FirstName, LastName, Gender) VALUES ('Tester', 'Testerson', 'M')
GO
INSERT INTO Person (FirstName, LastName, Gender) VALUES ('Jane',   'Doe',       'F')
GO
INSERT INTO Person (FirstName, LastName, MiddleName, Gender) VALUES ('Jürgen', 'König', 'Ko', 'M')
GO

-- Doctor Table Extension

CREATE TABLE Doctor(
    PersonID INTEGER     PRIMARY KEY  NOT NULL,
    Taxonomy VARCHAR(50) NOT NULL,
    FOREIGN KEY FK_Doctor_Person(PersonID) REFERENCES Person
)
GO
INSERT INTO Doctor (PersonID, Taxonomy) VALUES (1, 'Psychiatry')
GO

DROP TABLE MasterTable
GO
DROP TABLE SlaveTable
GO
CREATE TABLE MasterTable(
    ID1 INTEGER NOT NULL,
    ID2 INTEGER NOT NULL,
    PRIMARY KEY (ID1,ID2)
)
GO
CREATE TABLE SlaveTable(
    ID1    INTEGER NOT NULL,
    ID2222222222222222222222 INTEGER NOT NULL,
    ID2222222222222222           INTEGER NOT NULL,
    FOREIGN KEY FK_SlaveTable_MasterTable (ID2222222222222222222222, ID1)
    REFERENCES MasterTable
)
GO

CREATE TABLE Patient
(
    PersonID  INTEGER      PRIMARY KEY NOT NULL,
    Diagnosis VARCHAR(256) NOT NULL,

    FOREIGN KEY FK_Patient_Person (PersonID) REFERENCES Person
)
GO
INSERT INTO Patient(PersonID, Diagnosis) VALUES (2, 'Hallucination with Paranoid Bugs'' Delirium of Persecution')
GO
DROP TABLE Parent
GO
DROP TABLE Child
GO
DROP TABLE GrandChild
GO
CREATE TABLE Parent      (ParentID int, Value1 int)
GO
CREATE TABLE Child       (ParentID int, ChildID int)
GO
CREATE TABLE GrandChild  (ParentID int, ChildID int, GrandChildID int)
GO
DROP TABLE LinqDataTypes
GO
CREATE TABLE LinqDataTypes(
    ID             int,
    MoneyValue     decimal(10, 4),
    DateTimeValue  timestamp,
    DateTimeValue2 timestamp  Default NULL,
    BoolValue      smallint,
    GuidValue      varchar(38),
    BinaryValue    blob(5000) Default NULL,
    SmallIntValue  smallint,
    IntValue       int        Default NULL,
    BigIntValue    bigint     Default NULL,
    StringValue    VARCHAR(50) Default NULL
)
GO

DROP TABLE TestMerge1
GO
DROP TABLE TestMerge2
GO

CREATE TABLE TestMerge1
(
    Id       INTEGER            PRIMARY KEY NOT NULL,
    Field1   INTEGER                            ,
    Field2   INTEGER                            ,
    Field3   INTEGER                            ,
    Field4   INTEGER                            ,
    Field5   INTEGER                            ,

    FieldInt64      BIGINT                      ,
    FieldBoolean    SMALLINT                    ,
    FieldString     VARCHAR(20)                 ,
    FieldNString    NVARCHAR(20)                ,
    FieldChar       CHAR(1)                     ,
    FieldNChar      NCHAR(1)                    ,
    FieldFloat      REAL                        ,
    FieldDouble     DOUBLE                      ,
    FieldDateTime   TIMESTAMP                   ,
    FieldBinary     VARCHAR(20)  FOR BIT DATA       ,
    FieldGuid       varchar(38)                     ,
    FieldDecimal    DECIMAL(24, 10)             ,
    FieldDate       DATE                        ,
    FieldTime       TIME                        ,
    FieldEnumString VARCHAR(20)                 ,
    FieldEnumNumber INT                         
)
GO
CREATE TABLE TestMerge2
(
    Id       INTEGER            PRIMARY KEY NOT NULL,
    Field1   INTEGER                            ,
    Field2   INTEGER                            ,
    Field3   INTEGER                            ,
    Field4   INTEGER                            ,
    Field5   INTEGER                            ,

    FieldInt64      BIGINT                      ,
    FieldBoolean    SMALLINT                    ,
    FieldString     VARCHAR(20)                 ,
    FieldNString    NVARCHAR(20)                ,
    FieldChar       CHAR(1)                     ,
    FieldNChar      NCHAR(1)                    ,
    FieldFloat      REAL                        ,
    FieldDouble     DOUBLE                      ,
    FieldDateTime   TIMESTAMP                   ,
    FieldBinary     VARCHAR(20)  FOR BIT DATA       ,
    FieldGuid       varchar(38)                     ,
    FieldDecimal    DECIMAL(24, 10)             ,
    FieldDate       DATE                        ,
    FieldTime       TIME                        ,
    FieldEnumString VARCHAR(20)                 ,
    FieldEnumNumber INT                         
)
GO

DROP TABLE TestIdentity
GO
CREATE TABLE TestIdentity (
    ID   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL
)
GO
DROP TABLE AllTypes
GO
CREATE TABLE AllTypes(
      ID INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL

    , bigintDataType           bigint                  Default NULL
    , binaryDataType           binary(20)              Default NULL
    , blobDataType             blob                    Default NULL
    , charDataType             char(1)                 Default NULL
	, char20DataType           char(20)     CCSID 1208 Default NULL															
    , CharForBitDataType       char(5) for bit data    Default NULL
    , clobDataType             clob          CCSID 1208          Default NULL
    , dataLinkDataType         dataLink                Default NULL
    , dateDataType             date                    Default NULL
    , dbclobDataType           dbclob(100)   CCSID 1200          Default NULL
    , decfloat16DataType       decfloat(16)            Default NULL
    , decfloat34DataType       decfloat(34)            Default NULL
    , decimalDataType          decimal(30)             Default NULL
    , doubleDataType           double                  Default NULL
    , graphicDataType          graphic(10)  ccsid 13488           Default NULL
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
    , varGraphicDataType       vargraphic(10)  ccsid 13488        Default NULL
    , xmlDataType              xml                     Default NULL
)
GO

INSERT INTO AllTypes (bigintDataType) VALUES (NULL)

GO
INSERT INTO AllTypes(
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
    , xmlDataType              
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
    , 'graphic' -- DEFAULT --Cast('graphic' as graphic) --graphicDataType          gets error when casting the data
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
    , 'vargraphic'                    --varGraphicDataType
    , '<root><element strattr="strvalue" intattr="12345"/></root>' --xmlDataType  
)
GO

DROP VIEW PersonView
GO
CREATE VIEW PersonView
AS
SELECT * FROM Person
GO
DROP Procedure Person_SelectByKey
GO
CREATE Procedure Person_SelectByKey(in ID integer)
RESULT SETS 1
LANGUAGE SQL
BEGIN
    DECLARE C1 CURSOR FOR
        SELECT * FROM Person WHERE PersonID = ID;

    OPEN C1;
END
GO

DROP TABLE KeepIdentityTest
GO

CREATE TABLE KeepIdentityTest (
	ID    INTEGER  GENERATED ALWAYS AS IDENTITY PRIMARY KEY not null,
	intDataType INTEGER  
)
GO

GO
DROP TABLE AllTypes2
GO
CREATE TABLE AllTypes2(
	  ID INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY NOT NULL

	, bigintDataType           bigint                  Default NULL
	, binaryDataType           binary(20)              Default NULL
	, charDataType             char(1)                 Default NULL
	, char20DataType           char(20)     CCSID 1208 Default NULL
	, CharForBitDataType       char(5) for bit data    Default NULL
	, dataLinkDataType         dataLink                Default NULL
	, dateDataType             date                    Default NULL
	, decfloat16DataType       decfloat(16)            Default NULL
	, decfloat34DataType       decfloat(34)            Default NULL
	, decimalDataType          decimal(30)             Default NULL
	, doubleDataType           double                  Default NULL
	, graphicDataType          graphic(10) CCSID 13488 Default NULL
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
	, varGraphicDataType       vargraphic(10) CCSID 13488 Default NULL
)
GO
