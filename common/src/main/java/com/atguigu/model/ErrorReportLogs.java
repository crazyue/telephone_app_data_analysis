package com.atguigu.model;

/**
 * 应用上报的app错误日志相关信息
 */
public class ErrorReportLogs extends BasicLog {

	private static final long serialVersionUID = 1L;

	private String errorMajor;
	private String errorInDetail;
	private Long createTimeInMs;

	public String getErrorMajor() {
		return errorMajor;
	}

	public void setErrorMajor(String errorMajor) {
		this.errorMajor = errorMajor;
	}

	public String getErrorInDetail() {
		return errorInDetail;
	}

	public void setErrorInDetail(String errorInDetail) {
		this.errorInDetail = errorInDetail;
	}

	public Long getCreateTimeInMs() {
		return createTimeInMs;
	}

	public void setCreateTimeInMs(Long createTimeInMs) {
		this.createTimeInMs = createTimeInMs;
	}
}
