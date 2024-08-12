package br.com.sankhya.devcenter.precificacao.regra;

import br.com.sankhya.jape.bmp.PersistentLocalEntity;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.PrePersistEntityState;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.comercial.ContextoRegra;
import br.com.sankhya.modelcore.comercial.LiberacaoAlcadaHelper;
import br.com.sankhya.modelcore.comercial.LiberacaoSolicitada;
import br.com.sankhya.modelcore.comercial.Regra;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.ws.ServiceContext;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.BigDecimal;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import com.sankhya.util.JdbcUtils;

import java.sql.ResultSet;

public class ValidarVlrDigitadovPrecoMinimoRegra implements Regra {

    static final Logger logger = LogManager.getLogger(ValidarVlrDigitadovPrecoMinimoRegra.class);

    public ValidarVlrDigitadovPrecoMinimoRegra() {
        super();
        logger.isInfoEnabled();
    }


    @Override
    public void beforeInsert(ContextoRegra ctx) throws Exception {

    }

    @Override
    public void beforeUpdate(ContextoRegra ctx) throws Exception {

    }

    @Override
    public void beforeDelete(ContextoRegra ctx) throws Exception {

    }

    @Override
    public void afterInsert(ContextoRegra ctx) throws Exception {
        logger.info("Entrou no método afterInsert");
        PrePersistEntityState state = ctx.getPrePersistEntityState();
        final DynamicVO newVO = state.getNewVO();
        final boolean isCabecalho = newVO.getValueObjectID().indexOf(DynamicEntityNames.CABECALHO_NOTA) > -1;
        final boolean isItem = newVO.getValueObjectID().indexOf(DynamicEntityNames.ITEM_NOTA) > -1;
        final boolean isFinanceiro= newVO.getValueObjectID().indexOf(DynamicEntityNames.FINANCEIRO) > -1;

        if(isItem){
            final DynamicVO newVO1 = state.getNewVO();
            BigDecimal nuNota = (BigDecimal) newVO1.getProperty("NUNOTA");
            BigDecimal sequencia = (BigDecimal) newVO1.getProperty("SEQUENCIA");
            BigDecimal codProd = (BigDecimal) newVO1.getProperty("CODPROD");
            BigDecimal codLocal = (BigDecimal) newVO1.getProperty("CODLOCALORIG");
            String controle =  (String) newVO1.getProperty("CONTROLE");
            BigDecimal vlrUnitBrutoDigitado = (BigDecimal) newVO1.getProperty("AD_VLRUNITBRUTO");
            BigDecimal codUsu = ((AuthenticationInfo)ServiceContext.getCurrent().getAutentication()).getUserID();
            String tabela = "TGFITE";
            int evento = 1001;

            Boolean topConfigParaCalcularVlrUnitario = verificarConfiguracaoDaTop(nuNota);

            if(topConfigParaCalcularVlrUnitario){

                BigDecimal nuTab = NativeSql.getBigDecimal("TPV.AD_CODTABVLRMIN", "TGFTPV TPV, TGFCAB CAB", "TPV.CODTIPVENDA = CAB.CODTIPVENDA AND TPV.DHALTER = CAB.DHTIPVENDA AND CAB.NUNOTA = ?", new Object[]{nuNota});

                BigDecimal precoMinimo = buscarPrecoMinimo(nuTab, codProd, codLocal, controle);

                if(vlrUnitBrutoDigitado.compareTo(precoMinimo) < 0){
                    LiberacaoAlcadaHelper.apagaSolicitacoEvento(evento,nuNota,tabela,sequencia);

                    insereLiberacao(nuNota,sequencia,codProd,vlrUnitBrutoDigitado,precoMinimo,codUsu,controle,tabela,evento,ctx);

                } else if (vlrUnitBrutoDigitado.compareTo(precoMinimo) >= 0) {
                    LiberacaoAlcadaHelper.apagaSolicitacoEvento(evento,nuNota,tabela,sequencia);
                }

            }
        }
    }

    private void insereLiberacao(BigDecimal nuNota, BigDecimal sequencia, BigDecimal codProd, BigDecimal vlrUnitBrutoDigitado, BigDecimal precoMinimo, BigDecimal codUsu, String controle, String tabela,int evento,ContextoRegra ctx) throws Exception {
        logger.info("Entrou no método insereLiberacao");

        String descrProd = NativeSql.getString("PRO.DESCRPROD","TGFPRO PRO","CODPROD =?",new Object[]{codProd});
        String observacao = "O valor unitário bruto digitado para o produto " +codProd+ " - " +descrProd+ " é inferior ao preço mínimo informado na tabela de preços configurado no tipo de negociação.";

        LiberacaoSolicitada ls = new LiberacaoSolicitada(nuNota,tabela,evento,sequencia);

        ls.setDescricao(observacao);
        ls.setVlrLimite(precoMinimo);
        ls.setVlrTotal(BigDecimal.ZERO);
        ls.setVlrAtual(vlrUnitBrutoDigitado);
        ls.setLiberador(new BigDecimal(0));
        ls.setSolicitante(codUsu);
        ls.setSequenciaCascata(new BigDecimal(1));

        ctx.getBarramentoRegra().addLiberacaoSolicitada(ls);

        try {
            PersistentLocalEntity liberacaoEntity = LiberacaoAlcadaHelper.inserirSolicitacao(ls);
            if (liberacaoEntity != null) {
                // Sucesso na inserção, você pode adicionar código aqui para lidar com o sucesso, se necessário
                System.out.println("Solicitação inserida com sucesso no banco de dados.");
            } else {
                // Não foi necessário salvar, provavelmente por causa das verificações de cascata
                System.out.println("A solicitação não foi salva, verifique as condições de cascata.");
            }
        } catch (Exception e) {
            // Lida com qualquer exceção que ocorra durante a inserção
            e.printStackTrace();
        }
    }


    private BigDecimal buscarPrecoMinimo(BigDecimal nuTab, BigDecimal codProd, BigDecimal codLocal, String controle) throws Exception {
        logger.info("Entrou no método buscarPrecoMinimo");

        JdbcWrapper jdbc = null;
        NativeSql sql = null;
        EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
        jdbc = entityFacade.getJdbcWrapper();
        ResultSet resultSet = null;

        try{
            sql = new NativeSql(jdbc);
            sql.appendSql("SELECT AD_FNC_BUSCAR_PRECO_MIN_MAX(:NUTAB,:CODPROD,:CONTROLE,:CODLOCAL) AS PRECOMIN FROM DUAL");

            sql.setNamedParameter("NUTAB", nuTab);
            sql.setNamedParameter("CODPROD", codProd);
            sql.setNamedParameter("CODLOCAL", codLocal);
            sql.setNamedParameter("CONTROLE", controle);

            resultSet = sql.executeQuery();

            if(resultSet.next()){
                return resultSet.getBigDecimal("PRECOMIN");
            }

        }catch (Exception e){
            throw new Exception("ERRO Método buscarPrecoMinimo: " + e.getMessage());
        }finally {
            JdbcUtils.closeResultSet(resultSet);
            NativeSql.releaseResources(sql);
            JdbcWrapper.closeSession(jdbc);
        }
        return BigDecimal.ZERO;
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
    public void afterUpdate(ContextoRegra ctx) throws Exception {
        logger.info("Entrou no método afterUpdate");
        PrePersistEntityState state = ctx.getPrePersistEntityState();
        final DynamicVO newVO = state.getNewVO();
        final boolean isCabecalho = newVO.getValueObjectID().indexOf(DynamicEntityNames.CABECALHO_NOTA) > -1;
        final boolean isItem = newVO.getValueObjectID().indexOf(DynamicEntityNames.ITEM_NOTA) > -1;
        final boolean isFinanceiro= newVO.getValueObjectID().indexOf(DynamicEntityNames.FINANCEIRO) > -1;

        if(isItem){
            final DynamicVO newVO1 = state.getNewVO();
            BigDecimal nuNota = (BigDecimal) newVO1.getProperty("NUNOTA");
            BigDecimal sequencia = (BigDecimal) newVO1.getProperty("SEQUENCIA");
            BigDecimal codProd = (BigDecimal) newVO1.getProperty("CODPROD");
            BigDecimal codLocal = (BigDecimal) newVO1.getProperty("CODLOCALORIG");
            String controle =  (String) newVO1.getProperty("CONTROLE");
            BigDecimal vlrUnitBrutoDigitado = (BigDecimal) newVO1.getProperty("AD_VLRUNITBRUTO");
            BigDecimal codUsu = ((AuthenticationInfo)ServiceContext.getCurrent().getAutentication()).getUserID();
            String tabela = "TGFITE";
            int evento = 1001;

            Boolean topConfigParaCalcularVlrUnitario = verificarConfiguracaoDaTop(nuNota);

            if(topConfigParaCalcularVlrUnitario){

                BigDecimal precoMinimo = buscarPrecoMinimo(nuNota,codProd,codLocal,controle);

                if(vlrUnitBrutoDigitado.compareTo(precoMinimo) < 0){

                    BigDecimal valorLiberado = buscarValorLiberado(nuNota,tabela,evento,sequencia,1,0);

                    if(valorLiberado.compareTo(BigDecimal.ZERO) == 0 || vlrUnitBrutoDigitado.compareTo(valorLiberado) < 0) {
                        LiberacaoAlcadaHelper.apagaSolicitacoEvento(evento,nuNota,tabela,sequencia);

                        insereLiberacao(nuNota,sequencia,codProd,vlrUnitBrutoDigitado,precoMinimo,codUsu,controle,tabela,evento,ctx);
                    }

                } else if (vlrUnitBrutoDigitado.compareTo(precoMinimo) >= 0) {
                    LiberacaoAlcadaHelper.apagaSolicitacoEvento(evento,nuNota,tabela,sequencia);
                }

            }
        }
    }

    private BigDecimal buscarValorLiberado(BigDecimal nuChave, String tabela, Integer evento, BigDecimal sequencia, Integer seqCascata, Integer nuCll) throws MGEModelException {
        JdbcWrapper jdbc = EntityFacadeFactory.getDWFFacade().getJdbcWrapper();
        NativeSql sql = new NativeSql(jdbc);
        ResultSet resultSet = null;
        BigDecimal valorLiberado = new BigDecimal(0);

        try {
            sql.appendSql("SELECT VLRLIBERADO FROM TSILIB WHERE NUCHAVE = :NUCHAVE AND TABELA = :TABELA AND EVENTO = :EVENTO AND SEQUENCIA = :SEQUENCIA AND SEQCASCATA = :SEQCASCATA AND NUCLL = :NUCLL AND DHLIB IS NOT NULL AND REPROVADO = 'N'");
            sql.setNamedParameter("NUCHAVE", nuChave);
            sql.setNamedParameter("TABELA", tabela);
            sql.setNamedParameter("EVENTO", evento);
            sql.setNamedParameter("SEQUENCIA", sequencia);
            sql.setNamedParameter("SEQCASCATA", seqCascata);
            sql.setNamedParameter("NUCLL", nuCll);
            resultSet = sql.executeQuery();

            if(resultSet.next()){
                valorLiberado = resultSet.getBigDecimal("VLRLIBERADO");
                return valorLiberado;
            }

        }catch(Exception e) {
            MGEModelException.throwMe(e);
        }finally {
            JdbcUtils.closeResultSet(resultSet);
            NativeSql.releaseResources(sql);
            JdbcWrapper.closeSession(jdbc);
        }
        return BigDecimal.ZERO;
    }

    @Override
    public void afterDelete(ContextoRegra ctx) throws Exception {

    }
}
