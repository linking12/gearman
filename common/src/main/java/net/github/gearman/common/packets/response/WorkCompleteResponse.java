package net.github.gearman.common.packets.response;

import net.github.gearman.constants.PacketType;

/**
 * Created with IntelliJ IDEA. User: jewart Date: 11/30/12 Time: 10:12 AM To change this template use File | Settings |
 * File Templates.
 */
public class WorkCompleteResponse extends WorkDataResponse {

    public WorkCompleteResponse(String jobhandle, byte[] data){
        super(jobhandle, data);
        this.type = PacketType.WORK_COMPLETE;
    }

    public WorkCompleteResponse(byte[] pktdata){
        super(pktdata);

        this.type = PacketType.WORK_COMPLETE;
    }
}
