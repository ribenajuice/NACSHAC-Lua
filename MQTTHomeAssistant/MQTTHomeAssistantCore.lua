require("user.MQTTHomeAssistantConfig")
require("json")

function MQTTStart()
	if not mclient then
    -- Lets build the array for the config to be sent to homeassistant/light/DeviceName/Load/config
		ConnectedTopic = ConnectedTopic or 'Local/Unit/'..MQTTDeviceName..'/Status'
    
    MQTTHomeAssistant = {
      ['name'] = '',
      ['command_topic'] = '',
      ['state_topic'] = '',
      ['payload_off'] = 0,
      ['payload_on'] = 255,
      ['payload_stop'] = 5,
      ['brightness_command_topic'] = '',
      ['brightness_state_topic'] = '',
      ['retain'] = true,
      ['on_command_type'] = '',
      ['availability'] = {
				['topic'] = ConnectedTopic,
        },
      ['device'] = {
        ['manufacturer'] = 'Clipsal by Schneider Electric',
        ['model'] = 'C-Bus',
        },
      
      }

    --Assemble master data array
    Config = {}
    Discovery = {}
    
    for k, v in pairs(MQTT) do
      device = {}
      Config[k] = {}
      Config[k].name = v.Name
      Config[k].command_topic = "out/"..v.MQTTName.."/binary"
      Config[k].state_topic = "in/"..v.MQTTName.."/binary"
      NameNoSpaces = string.gsub(v.Name, "%s+", "-")
      Config[k].config = "homeassistant/"..v.type.."/"..MQTTDeviceName.."/"..NameNoSpaces.."/config"
      
      --Lighting Specific
      if v.type == 'light' then
        if v.dimmable == true then
          Config[k].brightness_command_topic = "out/"..v.MQTTName.."/analogue"
          Config[k].brightness_state_topic = "in/"..v.MQTTName.."/analogue"
          Config[k].on_command_type = "brightness"
        elseif v.dimmable == false then
          --Config[k].brightness_command_topic = ""
          --Config[k].brightness_state_topic = ""
          Config[k].on_command_type = "last"
      	end
      end
      if v.type == 'cover' then
      	Config[k].payload_stop = MQTTHomeAssistant.payload_stop
        
      end
      Config[k].dimmable = v.dimmable or false
      Config[k].payload_on = v.payload_on or MQTTHomeAssistant.payload_on
      Config[k].payload_off = v.payload_off or MQTTHomeAssistant.payload_off
      Config[k].retain = v.retain or MQTTHomeAssistant.retain
      Config[k].objectID = v.ObjectID
      Config[k].availability = v.availability or MQTTHomeAssistant.availability
      Config[k].unique_id = NameNoSpaces
      device['manufacturer'] = MQTTHomeAssistant.device.manufacturer
      device['model'] = MQTTHomeAssistant.device.model
      device['name'] = v.Name
      device['identifiers'] = v.Name
      Config[k].device = device
      
      --Cover Specific
      
      
    end

    --Assemble data for Home Assistant Auto Discovery

    for k,v in pairs(Config) do
      Discovery[k] = {}
      Discovery[k].name = v['name']
      Discovery[k].command_topic = v.command_topic
      Discovery[k].state_topic = v.state_topic
      Discovery[k].payload_off = v.payload_off
      Discovery[k].payload_on = v.payload_on
      if v.brightness_command_topic ~= '' then
        Discovery[k].brightness_command_topic = v.brightness_command_topic
      end
      if v.brightness_state_topic ~= '' then
        Discovery[k].brightness_state_topic = v.brightness_state_topic
      end  
      Discovery[k].retain = v.retain
      Discovery[k].on_command_type = v.on_command_type
      Discovery[k].availability = v.availability
      Discovery[k].unique_id = v.unique_id
      Discovery[k].device = v.device
      Config[k].jsonconfig = json.encode(Discovery[k])
    end

    -- Assemble data for MQTT mapping
    statusmap = {}
    controlmap = {}
    for k,v in pairs(Config) do
      -- Build the statusmap, otherwise known as "In from C-Bus"
      statusmap[v.objectID] = {}
        -- Build the controlmap, otherwise known as "Out to C-Bus"
      controlmap[v.command_topic] = {}

      statusmap[v.objectID].state_topic = v.state_topic
      controlmap[v.command_topic].objectID = v.objectID
      if v.dimmable == true then
        statusmap[v.objectID].brightness_state_topic = v.brightness_state_topic
        controlmap[v.brightness_command_topic] = {}
        controlmap[v.brightness_command_topic].objectID = v.objectID

      end
      statusmap[v.objectID].dimmable = v.dimmable
      controlmap[v.command_topic].dimmable = v.dimmable
    end
    MQTTControl = {}
    for k,v in pairs(controlmap) do
      MQTTControl[k] = v.objectID
    end
		
    --log(MQTTControl)
    
    MQTTBinaryStatus = {}
    for k,v in pairs(statusmap) do
      MQTTBinaryStatus[k] = v.state_topic
    end

    --log(MQTTBinaryStatus)

    MQTTAnalogueStatus = {}
    for k,v in pairs(statusmap) do
      if v.brightness_state_topic then
        MQTTAnalogueStatus[k] = v.brightness_state_topic
      end
    end

		--log(MQTTAnalogueStatus)
    
    MQTTHomeAssistantDiscovery = {}
    for k,v in pairs(Config) do
      MQTTHomeAssistantDiscovery[v.config] = v.jsonconfig
    end

    --log(MQTTHomeAssistantDiscovery)
    
			
    
    
		mclient = require("mosquitto").new(MQTTDeviceName)
		socket = require('socket')
		lb = require('localbus').new(10)

		datatypes = {}
		values = {}

		

    --log(MQTTStatus)
    
		for addr, _ in pairs(MQTTBinaryStatus) do
			obj = grp.find(addr)
			if obj then
				datatypes[ addr ] = obj.datatype
				values[ addr ] = obj.value
			else
				log('object not found ' .. tostring(addr))
				MQTTBinaryStatus[ addr ] = nil
			end
		end

   function publish(topic, value)
     mclient:publish(topic, tostring(value), 1, true)
   end

   mclient.ON_CONNECT = function(status, rc, msg)
    connected = status
    if status then
      log('mqtt connected')
      publish(ConnectedTopic, 'online')
      for k,v in pairs(MQTTHomeAssistantDiscovery) do
          mclient:publish(k,v)
      end

      for topic, _ in pairs(MQTTControl) do
        mclient:subscribe(topic)
      end

      for addr, topic in pairs(MQTTBinaryStatus) do
        local value = values[ addr ]
        if value ~= nil then
          if value > 0 then
          	publish(topic, 255)
          else
            publish(topic, 0)
  				end
        end
      end
      for addr, topic in pairs(MQTTAnalogueStatus) do
        local value = values[ addr ]
        if value ~= nil then
          publish(topic, value)
        end
      end
    else
      log('mqtt on_connect failed ' .. tostring(msg))
      mclient:disconnect()
    end
   end

   mclient.ON_MESSAGE = function(mid, topic, data)
     --log(mid,topic,data)
     local addr = MQTTControl[ topic ]
     
     if addr then        
        if type(data) == "string" then
          grp.write(addr, tonumber(data))
          if statusmap[addr].dimmable == true then
            publish(statusmap[addr].brightness_state_topic, tonumber(data))
          end
        end
    	end
   end

   mclient.ON_DISCONNECT = function(status, rc, msg)
     if connected then
     connected = false
     log('mqtt disconnected ' .. tostring(msg))
     end
     mclientfd = nil
   end

   function mconnect()
     local status, rc, msg, fd

     status, rc, msg = mclient:connect(host, port)
     if not status then
      log('mqtt connect failed ' .. tostring(msg))
     end
     fd = mclient:socket()
     if fd then
      mclientfd = fd
     end
   end

  function busevent(event)
    local addr = event.dst
    local topic = MQTTBinaryStatus[ addr ]
    if MQTTAnalogueStatus[ addr ] then
      analoguetopic = MQTTAnalogueStatus[ addr ]
    end
    if topic and connected then
      local value = dpt.decode(event.datahex, datatypes[ addr ], event.dstraw ) 
      values[ addr ] = value
      if value > 0 then
      	publish(topic, 255)
      else
        publish(topic, 0)
      end
      if analoguetopic then
      	publish(analoguetopic, value)
      end
    end
   end

   lb:sethandler('groupwrite', busevent)
   lb:sethandler('groupresponse', busevent)
   busfd = socket.fdmaskset(lb:getfd(), 'r')

   mclient:login_set(username,password)
   mclient:will_set( ConnectedTopic, 'offline')

   mconnect()

   timer = require('timerfd').new(5)
   timerfd = socket.fdmaskset(timer:getfd(), 'r')

  end

  -- mqtt connected
  if mclientfd then
   mask = mclient:want_write() and 'rw' or 'r'
   mclientfdset = socket.fdmaskset(mclientfd, mask)
   res, busstat, timerstat, mclientstat =
   socket.selectfds(10, busfd, timerfd, mclientfdset)

  -- mqtt not connected
  else
   res, busstat, timerstat =
   socket.selectfds(10, busfd, timerfd)
  end

  if mclientstat then
   if socket.fdmaskread(mclientstat) then
    mclient:loop_read()
   end

   if socket.fdmaskwrite(mclientstat) then
    mclient:loop_write()
   end

  end

  if busstat then
   lb:step()
  end

  if timerstat then
   timer:read() -- clear armed timer
   if mclientfd then
    mclient:loop_misc()
   else
    mconnect()
   end
  end
end

function split(line,sep)
	local res = {}
	local pos = 1
	sep = sep or ','
	while true do 
		local c = string.sub(line,pos,pos)
		if (c == "") then break end
		if (c == '"') then
			local txt = ""
			repeat
				local startp,endp = string.find(line,'^%b""',pos)
				txt = txt..string.sub(line,startp+1,endp-1)
				pos = endp + 1
				c = string.sub(line,pos,pos) 
				if (c == '"') then txt = txt..'"' end 
			until (c ~= '"')
			table.insert(res,txt)
      assert(c == sep or c == "")
			pos = pos + 1
		else	
			local startp,endp = string.find(line,sep,pos)
			if (startp) then 
				table.insert(res,string.sub(line,pos,startp-1))
				pos = endp + 1
			else
				table.insert(res,string.sub(line,pos))
				break
			end 
		end
	end
	return res
end
