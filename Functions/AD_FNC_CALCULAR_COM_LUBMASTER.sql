create or replace FUNCTION AD_FNC_CALCULAR_COM_LUBMASTER(P_NUNOTA INT)
RETURN NUMBER
AS
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
/*
    Autor: Claiton Farias
    Data: Agosto/24
    Objetivo: Calcular a comissão das notas conforme regra da empresa, essa função é utilizada na fórmula de comissão dentro do Sankhya
*/
    DECLARE
    V_VLRCOMISSAO   FLOAT;
    V_VLRCOMFIM     FLOAT;
    V_QTDITENS      INT;

    BEGIN

    V_VLRCOMFIM := 0;

       SELECT
         SUM((CASE WHEN Z.VLRUNIT <= Z.MEDIAPRECO
            THEN ROUND(((Z.VLRUNIT * (ROUND(((Z.PERCCOMX + Z.PERCCOMY) / 2),3) / 100)) * Z.QTDNEG),3)
            ELSE ROUND(((Z.PRECOMAX * (ROUND(((Z.PERCCOMX + Z.PERCCOMY) / 2),3) / 100)) * Z.QTDNEG),3)
            END)) AS VLRCOMISSAO
            INTO V_VLRCOMISSAO
        FROM
        (
            WITH DADOS AS
                    (
                    SELECT
                        ITE.SEQUENCIA
                        , ITE.QTDNEG
                        , ITE.CODPROD
                        , ITE.CODLOCALORIG
                        , ITE.CONTROLE
                        , ITE.VLRUNIT
                        , TPV.AD_CODTABVLRMIN
                        , TPV.AD_CODTABVLRMAX
                        , COALESCE(VEN.AD_COMPRECMIN,0) AS PERCCOMMIN
                        , COALESCE(VEN.AD_COMPRECMAX,0) AS PERCCOMMAX
                        , ROUND(((COALESCE(VEN.AD_COMPRECMIN,0) + COALESCE(VEN.AD_COMPRECMAX,0)) / 2),3) AS MEDIACOMISSAO
                        , AD_FNC_BUSCAR_PRECO_MIN_MAX(AD_CODTABVLRMIN,ITE.CODPROD,ITE.CONTROLE,ITE.CODLOCALORIG) AS PRECOMIN
                        , AD_FNC_BUSCAR_PRECO_MIN_MAX(AD_CODTABVLRMAX,ITE.CODPROD,ITE.CONTROLE,ITE.CODLOCALORIG) AS PRECOMAX
                        , ROUND(((AD_FNC_BUSCAR_PRECO_MIN_MAX(TPV.AD_CODTABVLRMIN,ITE.CODPROD,ITE.CONTROLE,ITE.CODLOCALORIG) + AD_FNC_BUSCAR_PRECO_MIN_MAX(TPV.AD_CODTABVLRMAX,ITE.CODPROD,ITE.CONTROLE,ITE.CODLOCALORIG)) / 2),3) AS MEDIAPRECO
                    FROM TGFCAB CAB
                        INNER JOIN TGFTPV TPV ON (TPV.CODTIPVENDA = CAB.CODTIPVENDA AND TPV.DHALTER = CAB.DHTIPVENDA)
                        INNER JOIN TGFITE ITE ON (ITE.NUNOTA = CAB.NUNOTA)
                        INNER JOIN TGFVEN VEN ON (VEN.CODVEND = CAB.CODVEND)
                    WHERE CAB.NUNOTA = P_NUNOTA
                    )
                    SELECT
                        D.SEQUENCIA
                        , D.QTDNEG
                        , D.VLRUNIT
                        , D.PRECOMIN
                        , D.PRECOMAX
                        , D.PERCCOMMIN
                        , D.PERCCOMMAX
                        , D.MEDIAPRECO
                        , D.MEDIACOMISSAO

                        , (CASE WHEN D.VLRUNIT <= D.MEDIAPRECO
                            THEN (CASE WHEN D.MEDIAPRECO = 0 THEN 0 ELSE ROUND(((D.VLRUNIT * D.MEDIACOMISSAO) / D.MEDIAPRECO),3) END)
                            ELSE (CASE WHEN D.PRECOMAX = 0 THEN 0 ELSE ROUND(((D.VLRUNIT * D.PERCCOMMAX) / D.PRECOMAX),3) END)
                        END) AS PERCCOMX

                        , (CASE WHEN VLRUNIT <= D.MEDIAPRECO
                            THEN (CASE WHEN D.PERCCOMMIN = 0 THEN 0 ELSE ROUND(((D.VLRUNIT * D.PERCCOMMIN) / D.PRECOMIN),3) END)
                            ELSE (CASE WHEN D.MEDIAPRECO = 0 THEN 0 ELSE ROUND(((D.VLRUNIT * D.MEDIACOMISSAO) / D.MEDIAPRECO),3) END)
                        END) AS PERCCOMY

                    FROM DADOS D
    )Z;

       BEGIN
        SELECT
            COUNT(*)
            INTO V_QTDITENS
        FROM TGFITE ITE
        WHERE ITE.NUNOTA = P_NUNOTA;

        EXCEPTION WHEN NO_DATA_FOUND THEN
        V_QTDITENS :=1;
        END;

        V_VLRCOMFIM := (V_VLRCOMISSAO / V_QTDITENS);

        BEGIN
            AD_PRC_ATU_COM_ITEM_LUBMASTER(P_NUNOTA);
            EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
        END;

     RETURN V_VLRCOMFIM;

    END;

END;