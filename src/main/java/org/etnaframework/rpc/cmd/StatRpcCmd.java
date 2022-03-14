package org.etnaframework.rpc.cmd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.etnaframework.core.util.SystemInfo;
import org.etnaframework.core.util.TimeSpanStat;
import org.etnaframework.core.web.HttpEvent;
import org.etnaframework.core.web.annotation.Cmd;
import org.etnaframework.core.web.annotation.CmdPath;
import org.etnaframework.core.web.cmd.HttpCmd;
import org.etnaframework.core.web.constant.CmdCategory;
import org.etnaframework.rpc.server.RpcMappers;
import org.etnaframework.rpc.server.RpcMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

/**
 * 统计RPC调用情况
 *
 * @author BlackCat
 * @since 2016-08-08
 */
@Controller
@CmdPath("/stat/cache")
public class StatRpcCmd extends HttpCmd {

    @Autowired
    private RpcMappers rpcMappers;

    @Override
    @Cmd(desc = "显示rpc服务器接口列表", category = CmdCategory.SYSTEM)
    public void index(HttpEvent he) throws Throwable {
        boolean timesOrder = he.getBool("timesOrder", false);
        boolean avgOrder = he.getBool("avgOrder", false);

        Map<String, RpcMeta> rpc_urls_map = rpcMappers.getReverseRpcAllSortedMap();

        List<Entry<String, RpcMeta>> entry_list = new ArrayList<Entry<String, RpcMeta>>(rpc_urls_map.entrySet());

        // 按照各个字段排序
        // 按请求比例排序
        if (timesOrder) {
            List<Entry<String, RpcMeta>> sorting = new ArrayList<Entry<String, RpcMeta>>(entry_list);
            Collections.sort(sorting, new Comparator<Entry<String, RpcMeta>>() {

                @Override
                public int compare(Entry<String, RpcMeta> o1, Entry<String, RpcMeta> o2) {
                    return (int) (o2.getValue().getStat().getAllNum() - o1.getValue().getStat().getAllNum()); // 根据方法调用次数的大小排序
                }
            });
            entry_list = sorting;
        }
        // 按平均请求时长排序
        if (avgOrder) {
            List<Entry<String, RpcMeta>> sorting = new ArrayList<Entry<String, RpcMeta>>(entry_list);
            Collections.sort(sorting, new Comparator<Entry<String, RpcMeta>>() {

                @Override
                public int compare(Entry<String, RpcMeta> o1, Entry<String, RpcMeta> o2) {
                    TimeSpanStat o2Stat = o2.getValue().getStat();
                    TimeSpanStat o1Stat = o1.getValue().getStat();
                    long o2AllSpan = o2Stat.getAllSpan();
                    long o2AllNum = o2Stat.getAllNum();
                    long o1AllSpan = o1Stat.getAllSpan();
                    long o1AllNum = o1Stat.getAllNum(); /// times avg slow slow_avg max_span slow_span
                    return (int) ((o2AllNum == 0 ? 0 : o2AllSpan / o2AllNum) - (o1AllNum == 0 ? 0 : o1AllSpan / o1AllNum)); // 根据平均时长排序
                }
            });
            entry_list = sorting;
        }
        he.set("timesOrder", timesOrder ? "▼" : "");
        he.set("avgOrder", avgOrder ? "▼" : "");
        he.set("url", he.getRequestURL());
        he.set("title", SystemInfo.COMMAND_SHORT + " RPC");
        he.set("rpcs", entry_list);
        he.setAccessLogContent("[RPC List]");
        he.renderHtml("/org/etnaframework/rpc/cmd/stat_rpc.html");
    }
}
