require("user.MQTTConfig")
require("json")
function MQTTStart()
	if not mclient then
    -- This part is the CSV Import, it looks for the file name specified in the MQTTConfig library
		if CSVEnable == true then
			local aMQTTToCBus = {}
			local aCBusToMQTT = {}
			csvloop = 1
			for line in io.lines('home/ftp/'..filename) do
				if csvloop ~= 1 then
					splitrow = split(line,",")
					last = string.gsub(splitrow[4],"\n","")
					last = string.gsub(splitrow[4],"\r","")
					if splitrow[1] == "rw" then
						aMQTTToCBus[last] = splitrow[2]
						aCBusToMQTT[splitrow[2]] = splitrow[3]
					elseif splitrow[1] == "w" then
						aMQTTToCBus[last] = splitrow[2]  
					elseif splitrow[1] == "r" then
						aCBusToMQTT[splitrow[2]] = splitrow[3]
					end
				end
				csvloop = csvloop + 1
			end
			-- Ending of the CSV import
		-- control, topic -> address map  -- How to control C-Bus
		controlmap = aMQTTToCBus
    
		-- status, address -> topic map -- Provides status to other devices
		statusmap = aCBusToMQTT 
		else
			if MQTTToCBus then
				controlmap = MQTTToCBus
      end
			if CBusToMQTT then
				statusmap = CBusToMQTT
			end
		end
    
    
    
		mclient = require("mosquitto").new(clientName)
		socket = require('socket')
		lb = require('localbus').new(10)

		datatypes = {}
		values = {}

		ConnectedTopic = ConnectedTopic or 'Local/Unit/NAC/Status'
    InTopics = 'Local/MQTT/'..clientName..'/In'
    OutTopics = 'Local/MQTT/'..clientName..'/Out'
    
    
		for addr, _ in pairs(statusmap) do
			obj = grp.find(addr)
			if obj then
				datatypes[ addr ] = obj.datatype
				values[ addr ] = obj.value
			else
				log('object not found ' .. tostring(addr))
				statusmap[ addr ] = nil
			end
		end

   function publish(topic, value)
     mclient:publish(topic, tostring(value), 1, true)
   end

   mclient.ON_CONNECT = function(status, rc, msg)
    connected = status
    if status then
      log('mqtt connected')
      publish(ConnectedTopic, 'connected')
      publish(InTopics, json.encode(statusmap))
      publish(OutTopics, json.encode(controlmap))

      for topic, _ in pairs(controlmap) do
        mclient:subscribe(topic)
      end

      for addr, topic in pairs(statusmap) do
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
     
     local addr = controlmap[ topic ]
     if addr then        
        if type(data) == "number" then
          grp.write(addr, data)
        elseif type(data) == "string" then
          grp.write(addr,data)
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
    local topic = statusmap[ addr ]
    if topic and connected then
      local value = dpt.decode(event.datahex, datatypes[ addr ], event.dstraw ) 
      values[ addr ] = value
      publish(topic, value)
    end
   end

   lb:sethandler('groupwrite', busevent)
   lb:sethandler('groupresponse', busevent)
   busfd = socket.fdmaskset(lb:getfd(), 'r')

   mclient:login_set(username,password)
   mclient:will_set( ConnectedTopic, 'disconnected')

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
