package kafdrop.util;

import cn.hutool.json.JSONUtil;
import io.cmex.caishen.spot.core.common.commands.BaseCommand;
import io.cmex.caishen.spot.core.common.dto.BalanceDTO;
import io.cmex.caishen.spot.core.common.dto.BalanceFlowDTO;
import io.cmex.caishen.spot.core.common.dto.OrderDTO;
import io.cmex.caishen.spot.core.common.dto.SymbolQuoteDTO;
import io.cmex.caishen.spot.core.common.dto.Ticket;
import io.cmex.caishen.spot.core.common.enums.CommandTypeEnum;
import io.cmex.caishen.spot.core.common.mq.BatchSpotCoreMessage;
import io.cmex.caishen.spot.core.common.mq.SpotCoreMessage;
import io.cmex.caishen.spot.core.common.utils.SerializeUtils;
import lombok.Data;
import org.springframework.beans.BeanUtils;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class BoltSpotCoreMessageDeserializer implements MessageDeserializer {

  private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  @Override
  public String deserializeMessage(ByteBuffer buffer) {
    BatchSpotCoreMessage message = (BatchSpotCoreMessage) SerializeUtils.deSerialize(buffer.array(),
      BatchSpotCoreMessage.class);
    BatchSpotCoreMessageDTO batchSpotCoreMessageDTO = new BatchSpotCoreMessageDTO();
    batchSpotCoreMessageDTO.setTime(message.getTime());
    batchSpotCoreMessageDTO.setTimestamp(format.format(new Date(message.getTime())));
    ArrayList<SpotCoreMessageDTO> spotCoreMessageDTOS = new ArrayList<>(message.getMessageList().size());
    batchSpotCoreMessageDTO.setMessageList(spotCoreMessageDTOS);
    for (SpotCoreMessage item : message.getMessageList()) {
      SpotCoreMessageDTO dto = new SpotCoreMessageDTO();
      BeanUtils.copyProperties(item, dto);
      CommandTypeEnum type = CommandTypeEnum.getByCode(item.getCmdType());
      BaseCommand command = (BaseCommand) SerializeUtils.deSerialize(item.getOriginCmd(), type.commandType);
      dto.setCommandName(type.desc);
      dto.setCommand(command);
      spotCoreMessageDTOS.add(dto);
    }
    return JSONUtil.toJsonStr(batchSpotCoreMessageDTO);
  }

  @Data
  private static class BatchSpotCoreMessageDTO {
    private long time;
    private String timestamp;
    private List<BoltSpotCoreMessageDeserializer.SpotCoreMessageDTO> messageList;
  }

  @Data
  private static class SpotCoreMessageDTO {
    private long tid;
    private long handleTime;
    private String commandName;
    private BaseCommand command;
    private List<SymbolQuoteDTO> symbolQuoteList;
    private List<Ticket> ticketList;
    private List<BalanceDTO> balanceList;
    private List<BalanceFlowDTO> balanceFlowList;
    private List<OrderDTO> orderList;
  }
}
