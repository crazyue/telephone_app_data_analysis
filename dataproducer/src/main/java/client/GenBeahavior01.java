package client;

import com.alibaba.fastjson.JSONObject;
import common.ErrorReportLogs;
import common.PageVisitReportLogs;
import common.StartupReportLogs;

import java.util.Random;

public class GenBeahavior01 {

    // 随机数生成
    private static Random random = new Random();

    // 计数变量
    private static int userResetNum = 0;

    // 可选变量值
    private static String[] userIds = initUserIds();
    private static String[] userPlatforms = {"android", "ios"};
    private static String appId = "app00001";
    private static String[] appVersions = {"1.0.1", "1.0.2"};
    private static String[] cities = {"Beijing", "Shanghai", "Guangzhou", "Tianjin", "Hangzhou", "Shenzhen", "Hunan", "Xian", "Shenyang", "Xinjiang"};
    private static String[] pageIds = {"page1.html", "page2.html", "page3.html", "page4.html"};
    private static int[] pageVisitIndexs = {0, 1, 2, 3, 4};
    private static String[] nextPages = {"page1.html", "page2.html", "page3.html", "page4.html", null};
    private static Long[] stayDurationAtSecs = {new Long(10), new Long(50), new Long(100)};
    private static String[] errorMajors = {"at cn.lift.dfdf.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)", "at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)"};
    private static String[] errorInDetails = {"java.lang.NullPointerException\\n    " +
            "at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\n " +
            "at cn.lift.dfdf.web.AbstractBaseController.validInbound", "at cn.lift.dfdfdf.control.CommandUtil.getInfo(CommandUtil.java:67)\\n " +
            "at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\n" +
            " at java.lang.reflect.Method.invoke(Method.java:606)\\n"};

    public static void main(String[] args) {
        gather();
    }

    public static void gather() {

        try {
            for (int i = 1; i <= 200000000; i++) {

                String userId = userIds[random.nextInt(userIds.length - 1)];
                String userPlatform = userPlatforms[random.nextInt(userPlatforms.length)];

                // BeginTime生成
                Long beginTime = System.currentTimeMillis();

                StartupReportLogs appStartupLogs = initAppStartupLogs(userId, userPlatform, beginTime);

//                PageVisitReportLogs pageVisitReportLogs = initPageVisitReportLogs(userId, userPlatform, beginTime);
//
//                ErrorReportLogs errorReportLogs = initErrorReportLogs(userId, userPlatform, beginTime);


                String startupJson = JSONObject.toJSONString(appStartupLogs);
                UploadUtil.upload(startupJson, 0);

//                String pageJson = JSONObject.toJSONString(pageVisitReportLogs);
//                UploadUtil.upload(pageJson, 1);
//
//                String errorJson = JSONObject.toJSONString(errorReportLogs);
//                UploadUtil.upload(errorJson, 2);

                userResetNum++;
                Thread.sleep(1000);
            }
        } catch (Exception ex) {
            System.out.println(ex);
        }
    }

    /**
     * 获取用户id
     *
     * @return
     */
    private static String[] initUserIds() {
        String base = "user11";
        String[] result = new String[100];
        for (int i = 0; i < 100; i++) {
            result[i] = base + i + "";
        }
        return result;
    }

    /**
     * 启动上报日志自动生成
     *
     * @param beginTime
     * @return
     */
    private static StartupReportLogs initAppStartupLogs(String userId, String userPlatform, Long beginTime) {
        StartupReportLogs appStartupLog = new StartupReportLogs();

        appStartupLog.setUserId(userId);
        appStartupLog.setAppPlatform(userPlatform);
        appStartupLog.setAppId(appId);

        appStartupLog.setAppVersion(appVersions[random.nextInt(appVersions.length)]);
        appStartupLog.setStartTimeInMs(beginTime);
        // APP的使用时间限制在半小时以内
        String timeMid = String.valueOf(random.nextInt(1200000));
        appStartupLog.setActiveTimeInMs(Long.valueOf(timeMid));

        // user1199
        String subUserId = userId.substring(userId.length() - 1);
        int cityDetermin = Integer.parseInt(subUserId);
        appStartupLog.setCity(cities[cityDetermin]);

        return appStartupLog;
    }

    /**
     * 页面访问上报日志自动生成
     *
     * @param beginTime
     * @return
     */
    private static PageVisitReportLogs initPageVisitReportLogs(String userId, String userPlatform, Long beginTime) {
        PageVisitReportLogs appPageReportLog = new PageVisitReportLogs();

        appPageReportLog.setUserId(userId);
        appPageReportLog.setAppPlatform(userPlatform);
        appPageReportLog.setAppId(appId);

        String currentPageId = pageIds[random.nextInt(pageIds.length)];
        int pageVisitIndex = pageVisitIndexs[random.nextInt(pageVisitIndexs.length)];
        String nextPage = nextPages[random.nextInt(nextPages.length)];
        while (currentPageId.equals(nextPage)) {
            nextPage = nextPages[random.nextInt(nextPages.length)];
        }
        Long stayDurationSecs = stayDurationAtSecs[random.nextInt(stayDurationAtSecs.length)];

        appPageReportLog.setCurrentPage(currentPageId);
        appPageReportLog.setStayDurationInSecs(stayDurationSecs);
        appPageReportLog.setPageVisitIndex(pageVisitIndex);
        appPageReportLog.setNextPage(nextPage);

        int timeMid = random.nextInt(300000);
        Long createTime = beginTime + timeMid;
        appPageReportLog.setCreateTimeInMs(createTime);

        return appPageReportLog;
    }

    /**
     * 错误上报日志自动生成
     *
     * @param beginTime
     * @return
     */
    private static ErrorReportLogs initErrorReportLogs(String userId, String userPlatform, Long beginTime) {
        ErrorReportLogs errorReportLog = new ErrorReportLogs();

        errorReportLog.setUserId(userId);
        errorReportLog.setAppPlatform(userPlatform);
        errorReportLog.setAppId(appId);

        errorReportLog.setErrorMajor(errorMajors[random.nextInt(errorMajors.length)]);
        errorReportLog.setErrorInDetail(errorInDetails[random.nextInt(errorInDetails.length)]);

        int timeMid = random.nextInt(300000);
        Long createTime = beginTime + timeMid;
        errorReportLog.setCreateTimeInMs(createTime);

        return errorReportLog;
    }
}
