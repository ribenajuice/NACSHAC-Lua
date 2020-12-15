   MQTTToCBus = {
      ['out/Local/Lighting/Lounge'] = '0/56/0',
      ['out/Local/Lighting/Bedroom'] = '0/56/1',
      ['out/Local/Lighting/Dining Room'] = '0/56/2',
      ['out/Local/Lighting/Bathroom'] = '0/56/3',
      ['out/Local/Lighting/Laundry'] = '0/56/4',
      ['out/Local/Lighting/Kitchen'] = '0/56/5',

   }

   CBusToMQTT = {
      ['0/56/0'] = 'in/Local/Lighting/Lounge',
      ['0/56/1'] = 'in/Local/Lighting/Bedroom',
      ['0/56/2'] = 'in/Local/Lighting/Dining Room',
      ['0/56/3'] = 'in/Local/Lighting/Bathroom',
      ['0/56/4'] = 'in/Local/Lighting/Laundry',
      ['0/56/5'] = 'in/Local/Lighting/Kitchen',

   }

    host = '192.168.254.8'
    port = 1883 
    username = ''
    password = ''
    clientName = '5500NAC'
		--ConnectedTopic = 'a/b/c/d'
		CSVEnable = false
		--filename = 'MQTT.csv'
