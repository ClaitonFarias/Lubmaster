package br.com.sankhya.devcenter.precificacao.eventos;

import br.com.sankhya.extensions.eventoprogramavel.EventoProgramavelJava;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.bmp.PersistentLocalEntity;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.TransactionContext;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.RegraDinamicaHelper;
import br.com.sankhya.modelcore.comercial.CentralFinanceiro;
import br.com.sankhya.modelcore.comercial.LiberacaoAlcadaHelper;
import br.com.sankhya.modelcore.comercial.LiberacaoSolicitada;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import com.sankhya.util.JdbcUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.ResultSet;
import br.com.sankhya.modelcore.comercial.impostos.ImpostosHelpper;

public class CalcularVlrUnitarioSemIpiSt implements EventoProgramavelJava {

    static final Logger logger = LogManager.getLogger(CalcularVlrUnitarioSemIpiSt.class);

    public CalcularVlrUnitarioSemIpiSt() {
        super();
        logger.isInfoEnabled();
    }

    private JdbcWrapper jdbc = null;

    @Override
    public void beforeInsert(PersistenceEvent event) throws Exception {
        logger.info("Entrou no beforeInsert");
        try{
            DynamicVO itemVO = (DynamicVO) event.getVo();

            BigDecimal nuNota = itemVO.asBigDecimalOrZero("NUNOTA");
            Boolean calcularVlrUnitario = verificarConfiguracaoDaTop(nuNota);

            if(calcularVlrUnitario){
                calcularVlrUnitario(itemVO);
            }

        }catch (Exception e){
            throw new Exception("ERRO: " + e.getMessage());
        }
    }

    private void calcularVlrUnitario(DynamicVO itemVO) throws Exception {
        logger.info("Entrou no método calcularVlrUnitario");

        BigDecimal codProd = itemVO.asBigDecimalOrZero("CODPROD");
        int qtdCasasDecimaisValor = buscarQtdCasasDecimaisParaValor(codProd);
        BigDecimal vlrUnitario = itemVO.asBigDecimalOrZero("VLRUNIT").setScale(qtdCasasDecimaisValor, RoundingMode.HALF_EVEN);
        BigDecimal quantidade = itemVO.asBigDecimalOrZero("QTDNEG");
        BigDecimal vlrTotalIpi = itemVO.asBigDecimalOrZero("VLRIPI");
        BigDecimal vlrTotalSt = itemVO.asBigDecimalOrZero("VLRSUBST");
        BigDecimal aliqIpi = itemVO.asBigDecimalOrZero("ALIQIPI");
        BigDecimal vlrUnitarioSt, vlrUnitarioIpi, percIpiUnitario, percStUnitario, vlrUnitarioNew, vlrTotalNew;

        logger.info("Valor unitario: " + vlrUnitario);
        logger.info("Quantidade: " + quantidade);
        logger.info("Valor total IPI: " + vlrTotalIpi);
        logger.info("Valor total ST: " + vlrTotalSt);

        vlrUnitarioIpi = vlrTotalIpi.divide(quantidade, MathContext.DECIMAL128).setScale(6,RoundingMode.HALF_EVEN);
        vlrUnitarioSt = vlrTotalSt.divide(quantidade, MathContext.DECIMAL128).setScale(6, RoundingMode.HALF_EVEN);
        percIpiUnitario = vlrUnitarioIpi.divide(vlrUnitario, MathContext.DECIMAL128).setScale(6, RoundingMode.HALF_EVEN);
        percStUnitario = vlrUnitarioSt.divide(vlrUnitario, MathContext.DECIMAL128).setScale(6, RoundingMode.HALF_EVEN);

        vlrUnitarioNew = vlrUnitario.divide(new BigDecimal(1).add(percIpiUnitario).add(percStUnitario), MathContext.DECIMAL128).setScale(qtdCasasDecimaisValor, RoundingMode.HALF_EVEN);
        vlrTotalNew = (vlrUnitarioNew.multiply(quantidade)).setScale(qtdCasasDecimaisValor, RoundingMode.HALF_EVEN);

        itemVO.setProperty("VLRUNIT", vlrUnitarioNew);
        itemVO.setProperty("VLRTOT", vlrTotalNew);
        itemVO.setProperty("AD_VLRUNITBRUTO", vlrUnitario);

            if(vlrTotalIpi.compareTo(BigDecimal.ZERO) > 0){
                itemVO.setProperty("BASEIPI", vlrTotalNew);
                itemVO.setProperty("VLRIPI", vlrTotalNew.multiply(aliqIpi).divide(new BigDecimal(100), MathContext.DECIMAL128));
            }

    }

    private int buscarQtdCasasDecimaisParaValor(BigDecimal codProd) throws Exception {
        logger.info("Entrou no método buscarQtdCasasDecimaisParaValor");

        JdbcWrapper jdbc = null;
        NativeSql sql = null;
        EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
        jdbc = entityFacade.getJdbcWrapper();
        ResultSet resultSet = null;

        try{
            sql = new NativeSql(jdbc);
            sql.appendSql("SELECT COALESCE(PRO.DECVLR,2) AS DECVLR FROM TGFPRO PRO\n" +
                    "WHERE PRO.CODPROD = :CODPROD");
            sql.setNamedParameter("CODPROD", codProd);

            resultSet = sql.executeQuery();

            if(resultSet.next()){
                return resultSet.getInt("DECVLR");
            }

        }catch (Exception e){
            throw new Exception("ERRO Método buscarQtdCasasDecimaisParaValor: " + e.getMessage());
        }finally {
            JdbcUtils.closeResultSet(resultSet);
            NativeSql.releaseResources(sql);
            JdbcWrapper.closeSession(jdbc);
        }
        return 2;
    }

    private Boolean verificarConfiguracaoDaTop(BigDecimal nuNota) throws Exception {
        logger.info("Entrou no método verificarConfiguracaoDaTop");

        JdbcWrapper jdbc = null;
        NativeSql sql = null;
        EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
        jdbc = entityFacade.getJdbcWrapper();
        ResultSet resultSet = null;

        try{
            sql = new NativeSql(jdbc);
            sql.appendSql("SELECT 1 FROM TGFCAB CAB\n" +
                    "INNER JOIN TGFTOP TP ON (TP.CODTIPOPER = CAB.CODTIPOPER AND TP.DHALTER = CAB.DHTIPOPER)\n" +
                    "WHERE CAB.NUNOTA = :NUNOTA\n" +
                    "AND TP.AD_USADACUSPRECO = 'S'");
            sql.setNamedParameter("NUNOTA", nuNota);

            resultSet = sql.executeQuery();

            return resultSet.next();

        }catch (Exception e){
            throw new Exception("ERRO Método verificarConfiguracaoDaTop: " + e.getMessage());
        }finally {
            JdbcUtils.closeResultSet(resultSet);
            NativeSql.releaseResources(sql);
            JdbcWrapper.closeSession(jdbc);
        }

    }

    @Override
    public void beforeUpdate(PersistenceEvent event) throws Exception {
        logger.info("Entrou no beforeUpdate");
        try{
            DynamicVO itemVO = (DynamicVO) event.getVo();
            DynamicVO oldItemVO = (DynamicVO) event.getOldVO();

            BigDecimal codProd = itemVO.asBigDecimalOrZero("CODPROD");
            int qtdCasasDecimaisValor = buscarQtdCasasDecimaisParaValor(codProd);
            BigDecimal nuNota = itemVO.asBigDecimalOrZero("NUNOTA");
            BigDecimal vlrUnitario = itemVO.asBigDecimalOrZero("VLRUNIT").setScale(qtdCasasDecimaisValor,RoundingMode.HALF_EVEN);
            BigDecimal vlrUnitarioOld = oldItemVO.asBigDecimalOrZero("VLRUNIT").setScale(qtdCasasDecimaisValor,RoundingMode.HALF_EVEN);

            Boolean calcularVlrUnitario = verificarConfiguracaoDaTop(nuNota);

            if(calcularVlrUnitario && (vlrUnitarioOld.compareTo(vlrUnitario) != 0)){
                logger.info("Entrou no if do calcular do update. Vlr. Unit. Anterior: " + vlrUnitarioOld + " Vlr. Unit. Atual: " + vlrUnitario);
                calcularVlrUnitario(itemVO);
            }

        }catch (Exception e){
            throw new Exception("ERRO: " + e.getMessage());
        }
    }

    @Override
    public void beforeDelete(PersistenceEvent event) throws Exception {

    }

    @Override
    public void afterInsert(PersistenceEvent event) throws Exception {

        try {
            DynamicVO vo = (DynamicVO) event.getVo();
            BigDecimal nuNota = vo.asBigDecimalOrZero("NUNOTA");
            Boolean calcularVlrUnitario = verificarConfiguracaoDaTop(nuNota);

            if(calcularVlrUnitario) {

                final ImpostosHelpper impostosHelper = new ImpostosHelpper();
                impostosHelper.calcularImpostos(nuNota);
                impostosHelper.totalizarNota(nuNota);

                final CentralFinanceiro centralFinanceiro = new CentralFinanceiro();
                centralFinanceiro.inicializaNota(nuNota);
                centralFinanceiro.refazerFinanceiro();

            }

        } catch (Exception e) {
            MGEModelException.throwMe(e);
        }finally {
            JdbcWrapper.closeSession(jdbc);
        }

    }

    @Override
    public void afterUpdate(PersistenceEvent event) throws Exception {
        try {
            DynamicVO vo = (DynamicVO) event.getVo();
            BigDecimal nuNota = vo.asBigDecimalOrZero("NUNOTA");
            Boolean calcularVlrUnitario = verificarConfiguracaoDaTop(nuNota);

            if(calcularVlrUnitario) {

                final ImpostosHelpper impostosHelper = new ImpostosHelpper();
                impostosHelper.calcularImpostos(nuNota);
                impostosHelper.totalizarNota(nuNota);

                final CentralFinanceiro centralFinanceiro = new CentralFinanceiro();
                centralFinanceiro.inicializaNota(nuNota);
                centralFinanceiro.refazerFinanceiro();

            }

        } catch (Exception e) {
            MGEModelException.throwMe(e);
        }finally {
            JdbcWrapper.closeSession(jdbc);
        }
    }

    @Override
    public void afterDelete(PersistenceEvent event) throws Exception {

    }

    @Override
    public void beforeCommit(TransactionContext tranCtx) throws Exception {

    }
}
