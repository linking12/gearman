package net.github.gearman.common.packets.response;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import com.google.common.primitives.Ints;

import net.github.gearman.common.packets.request.EchoRequest;
import net.github.gearman.constants.GearmanConstants;
import net.github.gearman.constants.PacketType;

public class ErrorResponse extends ResponsePacket {

    private final byte[] data;

    public ErrorResponse(String data){
        this.type = PacketType.ERROR;
        byte[] dataBytes = data.getBytes(GearmanConstants.CHARSET);
        this.data = dataBytes.clone();
    }

    public ErrorResponse(byte[] data){
        super(data);
        int pOff = 0;
        this.data = Arrays.copyOfRange(rawdata, pOff, rawdata.length);
        this.type = PacketType.ERROR;
    }

    @Override
    public byte[] toByteArray() {
        return concatByteArrays(getHeader(), data);
    }

    @Override
    public int getPayloadSize() {
        return data.length;
    }

    public byte[] getData() {
        return data;
    }

    public static void main(String[] args) {

        ErrorResponse error = new ErrorResponse("Ok");
        byte[] bytes = error.toByteArray();

        ErrorResponse errorOut = new ErrorResponse(bytes);
        try {
            byte[] errorInput = "Ok".getBytes();
            System.out.println(errorInput);
            System.out.println(new String(errorOut.getData(), "utf-8"));
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
