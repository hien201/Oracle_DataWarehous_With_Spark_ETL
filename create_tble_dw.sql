
--drop table TM_PORT_D;
--drop table TM_COUNTRY_D;
-- CREATE TABLE PORT_DIM
CREATE TABLE  TM_PORT_D (
                        PORT_CD         CHAR(3),
                        PORT_NM       CHAR(100),
                        STA_CD          CHAR(3),
                        LST_MDF_DATA    CHAR(8)
                       );


CREATE TABLE TM_DATE_D (
                        ARRV_DT         CHAR(8),
                        YR              CHAR(4),
                        MON             CHAR(2),
                        LST_MDF_DATA    CHAR(8)         
                        );

CREATE TABLE TM_VISA_D (
                       VISA_CD          NUMBER(8),
                       VISA_TP          CHAR(8),
                       LST_MDF_DATA     CHAR(8) 
                       );

CREATE TABLE TM_MODE_D (
                        MODE_CD         CHAR(3),
                        MODE_NM         CHAR(50),
                        LST_MDF_DATA    CHAR(8)
                        );

CREATE TABLE TM_STATE_D (
                        STA_CD          CHAR(3),
                        STA_NM          CHAR(100),
                        LST_MDF_DATA    CHAR(8)
                        );

CREATE TABLE TM_COUNTRY_D (
                        CNTR_CD         NUMBER(3),
                        CNTR_NM         CHAR(100),
                        LST_MDF_DATA    CHAR(8)
                          );

CREATE TABLE TS_IMGT (
                        I94_ID              NUMBER(6),
                        YR              CHAR(5),
                        MON             CHAR(4),
                        CIT             NUMBER(5),
                        RES             NUMBER(5),
                        PO_CD            CHAR(10),
                        ARRDT           CHAR(10),
                        MODE_CD            NUMBER(2),
                        STA_CD           CHAR(4),
                        DEPDT           CHAR(10),
                        BIR             NUMBER(3),
                        VISA_CD         CHAR(3),
                        DTADFILE        CHAR(8),
                        OCCUP           CHAR(100),
                        GENDER          CHAR(4),
                        FLTNO           CHAR(5),
                        VISA_TP         CHAR(4),
                        LST_MDF_DATA    CHAR(8)


);

CREATE TABLE TS_AIRPORT (
  ARPT_CD       CHAR(8),
  ARPT_TP     CHAR(80),
  ARPT_NM       CHAR(80),
  ELE_FT        NUMBER(5),
  CNTR          CHAR(80),
  MUN           CHAR(80),
  GPS_CD        CHAR(10),
  LOCAL_CD      CHAR(10),
  STA_CD        CHAR(5),
  LST_MDF_DATA  CHAR(8)
);


CREATE TABLE TS_TEMP (
                        STA_CD               CHAR(10),
                        CIT                CHAR(20),
                        MON                CHAR(10),
                        DAY                CHAR(10),
                        YR                 CHAR(6),
                        AVG_TEMP           NUMBER(4,2),
                        LST_MDF_DATA       CHAR(8)
                        );


CREATE TABLE TS_CITY_DEMO (
                           CIT            CHAR(10),
                           MD_AG          NUMBER(4),
                           M_PPLT         NUMBER(10),
                           TT_PPLT        NUMBER(10),
                           TT_PPLT        NUMBER(10),
                           NUM_VTR        NUMBER(10),
                           FRN_BR          NUMBER(10),
                           AVR_HOSE_SZ    NUMBER(5),
                           RAC            CHAR(40),
                           CNTR           NUMBER(10),
                           STA_CD         CHAR(30),
                           LST_MDF_DATA   CHAR(8) 
);


                          
                          

