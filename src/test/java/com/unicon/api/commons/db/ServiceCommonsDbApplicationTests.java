package com.unicon.api.commons.db;

import com.unicon.api.commons.db.dao.DaoApplication;
import com.unicon.api.commons.db.dao.mapper.AppMapper;
import lombok.extern.log4j.Log4j2;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 *
 * @author acrispin
 */
@SpringBootTest
@Log4j2
class ServiceCommonsDbApplicationTests {

	// private static Logger log = LoggerFactory.getLogger(ServiceCommonsDbApplicationTests.class);

	@Test
	void contextLoads() {
		log.info("test -----------");
		String server = "", username = "", sessionId = "", enviroment = "";
		try (SqlSession session = DaoApplication.getSqlSessionFactory().openSession(true)) {
			AppMapper mapper = session.getMapper(AppMapper.class);
			server = mapper.selectServer();
			username = mapper.selectUsername();
			sessionId = mapper.selectSessionId();
			enviroment = session.getConfiguration().getEnvironment().getId();
		} catch (PersistenceException | NullPointerException ex) {
			log.error(ex.getMessage(), ex);
		}
		log.info("server: " + server);
		log.info("username: " + username);
		log.info("sessionId: " + sessionId);
		log.info("enviroment: " + enviroment);
	}

}
