create or replace PROCEDURE AD_PRC_ATU_COM_ITEM_LUBMASTER(P_NUNOTA INT) AS
BEGIN
DECLARE
    V_PERCCOMX  FLOAT;
    V_PERCCOMY  FLOAT;
    V_PERCCOMISSAO  FLOAT;
    V_VLRCOMISSAO   FLOAT;
/*
    Autor: Claiton Farias
    Data: Agosto/24
    Objetivo: Usada para atualizar o percentual de comissão e valor de comissão encontrada para o item, ela é utilizada dentro da função AD_FNC_CALCULAR_COM_LUBMASTER.
*/
    BEGIN
        FOR X IN (WITH DADOS AS
        (
            SELECT
                ITE.SEQUENCIA
                , ITE.CODPROD
                , ITE.QTDNEG
                , ITE.CODLOCALORIG
                , ITE.CONTROLE
                , ITE.VLRUNIT
                , TPV.AD_CODTABVLRMIN
                , TPV.AD_CODTABVLRMAX
                , COALESCE(VEN.AD_COMPRECMIN,0) AS PERCCOMMIN
                , COALESCE(VEN.AD_COMPRECMAX,0) AS PERCCOMMAX
                , AD_FNC_BUSCAR_PRECO_MIN_MAX(AD_CODTABVLRMIN,ITE.CODPROD,ITE.CONTROLE,ITE.CODLOCALORIG) AS PRECOMIN
                , AD_FNC_BUSCAR_PRECO_MIN_MAX(AD_CODTABVLRMAX,ITE.CODPROD,ITE.CONTROLE,ITE.CODLOCALORIG) AS PRECOMAX
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
            , ROUND(((D.PRECOMIN + D.PRECOMAX) / 2),3) AS MEDIAPRECO
            , ROUND(((D.PERCCOMMIN + D.PERCCOMMAX) / 2),3) AS MEDIACOMISSAO
        FROM DADOS D)

       LOOP
            IF X.VLRUNIT <= X.MEDIAPRECO THEN
                V_PERCCOMX := (CASE WHEN X.MEDIAPRECO = 0 THEN 0 ELSE ROUND(((X.VLRUNIT * X.MEDIACOMISSAO) / X.MEDIAPRECO),3) END);
                V_PERCCOMY := (CASE WHEN X.PERCCOMMIN = 0 THEN 0 ELSE ROUND(((X.VLRUNIT * X.PERCCOMMIN) / X.PRECOMIN),3) END);
                V_PERCCOMISSAO := ROUND(((V_PERCCOMX + V_PERCCOMY) / 2),3);
                V_VLRCOMISSAO := ROUND(((X.VLRUNIT * (V_PERCCOMISSAO / 100)) * X.QTDNEG),3);

                UPDATE TGFITE SET AD_PERCCOMISSAO = V_PERCCOMISSAO, AD_VLRCOMISSAO = V_VLRCOMISSAO WHERE NUNOTA = P_NUNOTA AND SEQUENCIA = X.SEQUENCIA;
                COMMIT;

            ELSIF X.VLRUNIT > X.MEDIAPRECO THEN
                V_PERCCOMX := (CASE WHEN X.PRECOMAX = 0 THEN 0 ELSE ROUND(((X.VLRUNIT * X.PERCCOMMAX) / X.PRECOMAX),3) END);
                V_PERCCOMY := (CASE WHEN X.MEDIAPRECO = 0 THEN 0 ELSE ROUND(((X.VLRUNIT * X.MEDIACOMISSAO) / X.MEDIAPRECO),3) END);
                V_PERCCOMISSAO := ROUND(((V_PERCCOMX + V_PERCCOMY) / 2),3);
                V_VLRCOMISSAO := ROUND(((X.PRECOMAX * (V_PERCCOMISSAO / 100)) * X.QTDNEG),3);

                UPDATE TGFITE SET AD_PERCCOMISSAO = V_PERCCOMISSAO, AD_VLRCOMISSAO = V_VLRCOMISSAO WHERE NUNOTA = P_NUNOTA AND SEQUENCIA = X.SEQUENCIA;
                COMMIT;
            END IF;

       END LOOP;
    END;

END;