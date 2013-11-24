package com.pxf.infra.common;

import java.io.IOException;

import systemobject.terminal.Prompt;

import com.aqua.sysobj.conn.CliCommand;
import com.aqua.sysobj.conn.LinuxDefaultCliConnection;
import com.pxf.infra.utils.jsystem.report.ReportUtils;

/**
 * Representing Shell required system objects.
 * 
 */
public class ShellSystemObject extends LinuxDefaultCliConnection {

	private String host = "localHost";

	private String userName;

	private String password;

	private String lastCmdResult = "";

	private final long _30_SECONDS = 60 * 1000 * 2;

	private long timeOutCommand = _30_SECONDS;

	@Override
	public void init() throws Exception {

		super.setHost(host);
		super.setUser(userName);
		super.setPassword(password);

		Prompt p = new Prompt();
		p.setCommandEnd(true);
		p.setPrompt("$");

		addPrompts(new Prompt[] { p });
		super.init();
	}

	/**
	 * execute command-line command and store the result in lastCmdResult.
	 * 
	 * @param command
	 *            command to execute
	 * 
	 * @throws IOException
	 */
	protected void runCommand(String command) throws IOException {

		ReportUtils.startLevel(report, getClass(), command);

		CliCommand cmd = new CliCommand();
		cmd.setTimeout(timeOutCommand);
		cmd.setCommand(command);

		command(cmd);

		lastCmdResult = cmd.getResult();

		ReportUtils.report(report, getClass(), lastCmdResult);

		ReportUtils.stopLevel(report);
	}

	public String getLastCmdResult() {
		return lastCmdResult;
	}

	public void setLastCmdResult(String lastCmdResult) {
		this.lastCmdResult = lastCmdResult;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	protected void jps() throws IOException {
		runCommand("jps");
	}

	public long getTimeOutCommand() {
		return timeOutCommand;
	}

	public void setTimeOutCommand(long timeOutCommand) {
		this.timeOutCommand = timeOutCommand;
	}
}
