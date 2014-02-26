package com.threeglav.sh.bauk.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;

public class EmailSender {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final String allErrorRecipients = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.EMAIL_RECIPIENTS_LIST_PARAM_NAME, null);

	private final JavaMailSenderImpl mailSender;

	private final boolean canSendEmails;

	public EmailSender() {
		mailSender = new JavaMailSenderImpl();
		final String emailHost = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.EMAIL_HOST_PARAM_NAME, "localhost");
		if (StringUtil.isEmpty(emailHost)) {
			throw new IllegalStateException("Unable to send emails when " + BaukEngineConfigurationConstants.EMAIL_HOST_PARAM_NAME
					+ " property was not set");
		}
		mailSender.setHost(emailHost);
		log.info("Will use email server host {}", emailHost);
		final int port = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.EMAIL_HOST_PORT_PARAM_NAME, -1);
		if (port > -1) {
			mailSender.setPort(port);
			log.info("Will use email port {}", port);
		}
		final String username = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.EMAIL_USERNAME_PARAM_NAME, null);
		if (!StringUtil.isEmpty(username)) {
			mailSender.setUsername(username);
		}
		final String pass = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.EMAIL_PASSWORD_PARAM_NAME, null);
		if (!StringUtil.isEmpty(pass)) {
			mailSender.setPassword(pass);
		}
		canSendEmails = !StringUtil.isEmpty(allErrorRecipients);
	}

	public void sendProcessingErrorEmail(final String text) {
		if (!canSendEmails) {
			return;
		}
		final SimpleMailMessage message = new SimpleMailMessage();
		message.setFrom("BaukEtlEngine");
		message.setSubject("BaukEtl - processing error");
		String[] recipients = null;
		if (!StringUtil.isEmpty(allErrorRecipients)) {
			recipients = allErrorRecipients.split(",");
		}
		if (recipients == null) {
			log.error("Unable to send error email. No recipients configured!");
			return;
		}
		message.setTo(recipients);
		message.setText(text);
		try {
			mailSender.send(message);
			log.debug("Sent email message {}", text);
		} catch (final Exception exc) {
			log.error("Exception while sending email", exc);
		}
	}

}
