package common;

/**
 * 应用上报的页面相关信息
 */
public class PageVisitReportLogs extends BasicLog {

    private static final long serialVersionUID = 1L;

    private int pageViewCntInSession = 0;
    private String CurrentPage;
    private int pageVisitIndex = 0;
    private String nextPage;
    private Long stayDurationInSecs = 0L;
    private Long createTimeInMs = 0L;

    public Long getCreateTimeInMs() {
        return createTimeInMs;
    }

    public void setCreateTimeInMs(Long createTimeInMs) {
        this.createTimeInMs = createTimeInMs;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public int getPageViewCntInSession() {
        return pageViewCntInSession;
    }

    public void setPageViewCntInSession(int pageViewCntInSession) {
        this.pageViewCntInSession = pageViewCntInSession;
    }

    public String getCurrentPage() {
        return CurrentPage;
    }

    public void setCurrentPage(String currentPage) {
        CurrentPage = currentPage;
    }

    public int getPageVisitIndex() {
        return pageVisitIndex;
    }

    public void setPageVisitIndex(int pageVisitIndex) {
        this.pageVisitIndex = pageVisitIndex;
    }

    public String getNextPage() {
        return nextPage;
    }

    public void setNextPage(String nextPage) {
        this.nextPage = nextPage;
    }

    public Long getStayDurationInSecs() {
        return stayDurationInSecs;
    }

    public void setStayDurationInSecs(Long stayDurationInSecs) {
        this.stayDurationInSecs = stayDurationInSecs;
    }
}
