package com.jiohealth.dao.services;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;

import com.jiohealth.dao.entity.ClientMaster;
import com.jiohealth.dao.entity.ClientReportDetails;
import com.jiohealth.dao.entity.ErrorDataModel;
import com.jiohealth.dao.services.inter.DAOMaster;
import com.jiohealth.db.utils.PostgresDBConnection;
import com.jiohealth.resource.KeyBundle;
import com.jiohealth.util.Constants;
import com.jiohealth.util.PropertyHandler;

public class DAOService extends DAOMaster
{
	private static final Logger logger = Logger.getLogger(DAOService.class);
	
	public List<ClientReportDetails> save ( List<ClientReportDetails> clientReportDetailsList ) throws Exception 
	{
		Session session=null;
		ClientReportDetails clientReportDetails=null;
		List<ClientReportDetails> savedClientReportDetails=new ArrayList<ClientReportDetails>();
		
		try{
			session = PostgresDBConnection.getSessionFactory().openSession();
			
			session.beginTransaction();
			for(ClientReportDetails clientRepDetails:clientReportDetailsList){
					session.save(clientRepDetails);
					savedClientReportDetails.add(clientReportDetails);
			}
			session.getTransaction().commit();
			
			return clientReportDetailsList;
		}catch(Exception e ){
			logger.error("Exception occur while saving data to DB",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
	}
	
	public ClientReportDetails save( ClientReportDetails clientReportDetailsList ) throws Exception 
	{
		Session session=null;
		try{
			session = PostgresDBConnection.getSessionFactory().openSession();
			
			session.beginTransaction();
			session.save(clientReportDetailsList);
			session.getTransaction().commit();
			
			return clientReportDetailsList;
		}catch(Exception e ){
			logger.error("Exception occur while saving data to DB",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
	}
	
	@SuppressWarnings("unchecked")
	public List<ClientReportDetails> getLabIdTobeProcessed(final String clientCode) throws Exception {
		
		Session session =null;
		List<ClientReportDetails> clientReportDetails =null;
		
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			Criteria criteria = session.createCriteria(ClientReportDetails.class);
			
			Criterion nextRetryTimeCheck= Restrictions.or(Restrictions.le("nextRetryTime", new Date()), 
			           Restrictions.isNull("nextRetryTime"));
			Criterion fileProcessedStatusCheck= Restrictions.or(Restrictions.eq("status", Constants.STATUS_NEW), 
					Restrictions.eq("status", Constants.STATUS_RETRY));
			
			ClientMaster clientMaster=fetchClientMaster(clientCode);
			
			Criterion adpaterId=Restrictions.eq("clientMaster", clientMaster);
								
			
			Criterion combinedBothConditions= Restrictions.and(nextRetryTimeCheck,fileProcessedStatusCheck);
			Criterion whereConditions= Restrictions.and(combinedBothConditions,adpaterId);
			
			criteria.add(whereConditions);
			if(PropertyHandler.getInstance().getValue(KeyBundle.LIMIT)!=null) {
				criteria.setMaxResults(Integer.parseInt(PropertyHandler.getInstance().getValue(KeyBundle.LIMIT)));
			}
			clientReportDetails =criteria.list();

			clientReportDetails=updateStatusToInProcess(clientReportDetails,session);
			
			return clientReportDetails;
		}catch(Exception e){
			logger.error("Exception occur in getLabIdTobeProcessed",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
	}
	
	@SuppressWarnings("unchecked")
	public List<ClientReportDetails> getLabIdTobeReady(final String clientCode) throws Exception {
		
		Session session =null;
		List<ClientReportDetails> clientReportDetails =null;
		
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			Criteria criteria = session.createCriteria(ClientReportDetails.class);
			
			
			Criterion fileProcessedStatusCheck= Restrictions.eq("status", Constants.STATUS_NEW);
			
			ClientMaster clientMaster=fetchClientMaster(clientCode);
			
			Criterion adpaterId=Restrictions.eq("clientMaster", clientMaster);
			
			Criterion whereConditions= Restrictions.and(fileProcessedStatusCheck,adpaterId);
			
			criteria.add(whereConditions);
			clientReportDetails =criteria.list();
			return clientReportDetails;
		}catch(Exception e){
			logger.error("Exception occur in getLabIdTobeProcessed",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
	}
	
	@SuppressWarnings("unchecked")
	public List<ClientReportDetails> getLabIdTobeProcesseds(final String clientCode) throws Exception {
		
		Session session =null;
		List<ClientReportDetails> clientReportDetails =null;
		
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			Criteria criteria = session.createCriteria(ClientReportDetails.class);
			
			Criterion nextRetryTimeCheck= Restrictions.or(Restrictions.le("nextRetryTime", new Date()), 
			           Restrictions.isNull("nextRetryTime"));
			Criterion fileProcessedStatusCheck= Restrictions.or(Restrictions.eq("status", "READY"), 
					Restrictions.eq("status", Constants.STATUS_RETRY));
			
			ClientMaster clientMaster=fetchClientMaster(clientCode);
			
			Criterion adpaterId=Restrictions.eq("clientMaster", clientMaster);
								
			
			Criterion combinedBothConditions= Restrictions.and(nextRetryTimeCheck,fileProcessedStatusCheck);
			Criterion whereConditions= Restrictions.and(combinedBothConditions,adpaterId);
			
			criteria.add(whereConditions);
			if(PropertyHandler.getInstance().getValue(KeyBundle.LIMIT)!=null) {
				criteria.setMaxResults(Integer.parseInt(PropertyHandler.getInstance().getValue(KeyBundle.LIMIT)));
			}
			clientReportDetails =criteria.list();

			clientReportDetails=updateStatusToInProcess(clientReportDetails,session);
			
			return clientReportDetails;
		}catch(Exception e){
			logger.error("Exception occur in getLabIdTobeProcessed",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
	}
	
	
	
	@SuppressWarnings("unchecked")
	public List<ClientReportDetails> updateTitleToReady(final String status) throws Exception {
		
		Session session =null;
		List<ClientReportDetails> clientReportDetails =null;
		
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			Criteria criteria = session.createCriteria(ClientReportDetails.class).setProjection(
			        Projections.distinct(Projections.property("patient_id")));
			
			Criterion whereConditions=Restrictions.eq("status", status);
			
			criteria.add(whereConditions);
		
			clientReportDetails =criteria.list();
			return clientReportDetails;
		}catch(Exception e){
			logger.error("Exception occur in getLabIdTobeProcessed",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
	}	
	
	private List<ClientReportDetails> updateStatusToInProcess(final List<ClientReportDetails> clientReportDetailsList,Session session ) throws Exception
	{
		try{
			session.beginTransaction();
			
			for(ClientReportDetails clientReportDetails:clientReportDetailsList){
				clientReportDetails.setStatus(Constants.STATUS_INPROGRESS);
				clientReportDetails.setUpdatedOn(new Date());
				session.update(clientReportDetails);
			}
			session.getTransaction().commit();
		}catch(Exception e){
			logger.error("Exception occur in updateStatusToInProcess",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
		return clientReportDetailsList;
	}
	
	public void updateStatusToReady(final ClientReportDetails clientReportDetails,final String status) throws Exception
	{
		Session session=null;
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			session.beginTransaction();

			clientReportDetails.setStatus(status);
			clientReportDetails.setUpdatedOn(new Date());
			session.update(clientReportDetails);
			
			session.getTransaction().commit();
		}catch(Exception e){
			logger.error("Exception occur in updateStatusToInProcess",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
	}
	public boolean updateFileStatus(final ClientReportDetails clientReportDetails,final String status,final Date retryDateTime) throws Exception
	{
		Session session=null;
		boolean flag=false;
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			session.beginTransaction();
			
			clientReportDetails.setStatus(status);
			clientReportDetails.setNextRetryTime(null);
			clientReportDetails.setUpdatedOn(new Date());
			
			if ( status.equalsIgnoreCase ( Constants.STATUS_RETRY ) || retryDateTime != null )
			{
				clientReportDetails.setNextRetryTime ( retryDateTime );
			}
			session.update(clientReportDetails);
			session.getTransaction().commit();
			flag=true;
		}catch(Exception e){
			logger.error("Exception occur in updateFileStatus",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
		return flag;
	}
	
	public boolean updateClientReportDetails(final ClientReportDetails clientReportDetails,final String status) throws Exception
	{
		Session session=null;
		boolean flag=false;
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			session.beginTransaction();
			
			String hql = "UPDATE client_report_details set status = :status "  + 
		             "WHERE observationid = :observationid";
		Query query = session.createQuery(hql);
		query.setParameter("status", status);
		query.setParameter("observationid", clientReportDetails.getObservationId());
		int result = query.executeUpdate();
		logger.info("RECORD UPDATE"+result);
		
			session.getTransaction().commit();
			flag=true;
		}catch(Exception e){
			logger.error("Exception occur in updateFileStatus",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
		return flag;
	}
	
	public boolean updateFileStatus(final ClientReportDetails clientReportDetails) throws Exception
	{
		Session session=null;
		boolean flag=false;
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			session.beginTransaction();			
			session.update(clientReportDetails);
			session.getTransaction().commit();
			flag=true;
		}catch(Exception e){
			logger.error("Exception occur in updateFileStatus",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
		return flag;
	}

	@SuppressWarnings("unchecked")
	public ClientMaster fetchClientMaster(final String adapterName) throws Exception
	{
		Session session=null;
		ClientMaster clientMaster=null;
		
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			Criteria criteria = session.createCriteria(ClientMaster.class);
			criteria.add(Restrictions.eq("clientCode", adapterName));
			
			List<ClientMaster> clientMasters=criteria.list();
			
			if(clientMasters!=null && clientMasters.size()>0){
				return clientMasters.get(0);
			}
			
		}catch(Exception e){
			logger.error("Exception occur in fetchClientMaster",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
		return clientMaster;
	}
	@SuppressWarnings("unchecked")
	public ClientReportDetails fetchClientReportDetailsTitle(final String status) throws Exception
	{
		Session session=null;
		ClientReportDetails clientReportDetail=null;
		
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			Criteria criteria = session.createCriteria(ClientReportDetails.class);
			criteria.add(Restrictions.eq("status", status));
			
			List<ClientReportDetails> clientReportDetails=criteria.list();
			
			if(clientReportDetails!=null && clientReportDetails.size()>0){
				return clientReportDetails.get(0);
			}
			
		}catch(Exception e){
			logger.error("Exception occur in fetchClientMaster",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
		return clientReportDetail;
	}
	
	public ClientReportDetails fetchClientReportDetails(final int reportDetailsId) throws Exception
	{
		Session session=null;
		ClientReportDetails clientReportDetails=null;
		
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			clientReportDetails=(ClientReportDetails)session.get(ClientReportDetails.class, reportDetailsId);
			
		}catch(Exception e){
			logger.debug("Exception occur in fetchClientReportDetails",e);
		}
		finally{
			closeSession(session);
		}
		return clientReportDetails;
	}
	
	public ClientMaster saveClientMaster(final ClientMaster clientMaster)  throws Exception
	{
		Session session=null;
		
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			session.beginTransaction();
			session.save(clientMaster);
			session.getTransaction().commit();
			
		}catch(Exception e){
			logger.error("Exception occur in saveClientMaster",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
		return clientMaster;
	}
	
	
	@SuppressWarnings("unchecked")
	public ClientReportDetails fetchClientReportDetails(final String visitorId) throws Exception
	{
		Session session=null;
		ClientReportDetails clientReportDetails=null;
		
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			Criteria criteria = session.createCriteria(ClientReportDetails.class);
			criteria.add(Restrictions.eq("reportId", visitorId));
			
			List<ClientReportDetails> clientReportDetailsList=criteria.list();
			
			if(clientReportDetailsList!=null && clientReportDetailsList.size()>0){
				return clientReportDetailsList.get(0);
			}
			
		}catch(Exception e){
			logger.error("Exception occur in fetchClientReportDetails",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
		return clientReportDetails;
	}
	
	@SuppressWarnings("unchecked")
	public ClientReportDetails fetchClientReportDetails(final String reportId,final String mobileNo,final String externalPatientId) throws Exception
	{
		Session session=null;
		ClientReportDetails clientReportDetails=null;
		
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			Criteria criteria = session.createCriteria(ClientReportDetails.class);
			
			Criterion mobileNoAndExternalPatientId= Restrictions.and(Restrictions.eq("patientPrimaryContactNo", mobileNo), Restrictions.eq("patientId", externalPatientId));
			
			Criterion conditionsChecks= Restrictions.and(Restrictions.eq("reportId", reportId),mobileNoAndExternalPatientId);
			
			criteria.add(conditionsChecks);
			
			List<ClientReportDetails> clientReportDetailsList=criteria.list();
			
			if(clientReportDetailsList!=null && clientReportDetailsList.size()>0){
				return clientReportDetailsList.get(0);
			}
			
		}catch(Exception e){
			logger.error("Exception occur in fetchClientReportDetails",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
		return clientReportDetails;
	}
	
	@SuppressWarnings("unchecked")
	public ClientReportDetails fetchClientReportDetails(final String reportId, final String observationId, final String mobileNo,final String externalPatientId) throws Exception
	{
		Session session=null;
		ClientReportDetails clientReportDetails=null;
		
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			Criteria criteria = session.createCriteria(ClientReportDetails.class);
			
			Criterion mobileNoAndExternalPatientId= Restrictions.and(Restrictions.eq("patientPrimaryContactNo", mobileNo), Restrictions.eq("patientId", externalPatientId));
			
			Criterion andReportId = Restrictions.and(Restrictions.eq("reportId", reportId),mobileNoAndExternalPatientId);
			
			Criterion conditionsChecks= Restrictions.and(Restrictions.eq("observationId", observationId), andReportId);
			
			criteria.add(conditionsChecks);
			
			List<ClientReportDetails> clientReportDetailsList=criteria.list();
			
			if(clientReportDetailsList!=null && clientReportDetailsList.size()>0){
				return clientReportDetailsList.get(0);
			}
			
		}catch(Exception e){
			logger.error("Exception occur in fetchClientReportDetails",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
		return clientReportDetails;
	}
	
	public ErrorDataModel save( ErrorDataModel errorDataModelList ) throws Exception 
	{
		Session session=null;
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			session.beginTransaction();
			session.save(errorDataModelList);
			session.getTransaction().commit();
			
			return errorDataModelList;
		}catch(Exception e ){
			logger.error("Exception occur while saving data to DB",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
	}
	
	public ErrorDataModel fetchErrorDataDetails(final String refId) throws Exception
	{
		Session session=null;
		ErrorDataModel errorDataModel=null;
		
		try{
			session=PostgresDBConnection.getSessionFactory().openSession();
			
			Criteria criteria = session.createCriteria(ErrorDataModel.class);
			criteria.add(Restrictions.eq("refId", refId));
			
			Object result = criteria.uniqueResult();
			
			if(result !=null){
				errorDataModel = (ErrorDataModel) result;
				return errorDataModel;
			}
			
		}catch(Exception e){
			logger.error("Exception occur in fetchErrorDataDetails",e);
			throw new Exception(e);
		}
		finally{
			closeSession(session);
		}
		return errorDataModel;
	}
	
	private void closeSession(Session session) {
		if(session.isOpen()){
			session.close();
		}
	}
}