import blpapi

class SyncSession(blpapi.Session):
    Event_dict = {
        1: 'ADMIN',
    2: 'SESSION_STATUS',
    3: 'SUBSCRIPTION_STATUS',
    4: 'REQUEST_STATUS',
    5: 'RESPONSE',
    6: 'PARTIAL_RESPONSE',
    8: 'SUBSCRIPTION_DATA',
    9: 'SERVICE_STATUS',
    10: 'TIMEOUT',
    11: 'AUTHORIZATION_STATUS',
    12: 'RESOLUTION_STATUS',
    13: 'TOPIC_STATUS',
    14: 'TOKEN_STATUS',
    15: 'REQUEST',
    -1: 'UNKNOWN'}
    
    def __init__(self, eventHandler = None, eventDispatcher = None, useUser = False, user = ('Corp/elvis', '192.168.50.105')) -> None:
        options = blpapi.SessionOptions()
        options.setServerHost('161.117.176.172')
        options.setServerPort(8194)
        options.setAutoRestartOnDisconnection(True)
        
        if not useUser:
            options.setSessionIdentityOptions(blpapi.AuthOptions.createWithApp('pollock:Dashboard'))
        else:
            user = blpapi.AuthUser.createWithManualOptions(user[0], user[1]) 
            options.setSessionIdentityOptions(blpapi.AuthOptions.createWithUserAndApp(user, 'pollock:Dashboard'))
        
        super().__init__(options, eventHandler, eventDispatcher)
        self.DataQueue = blpapi.EventQueue()
        self.start()
        
    def sendRequest(self, requests, useDefaultQueue = False, timeout = 10, print_msg = False):
        for request in requests:
            if useDefaultQueue:
                super().sendRequest(**request)
            else:    
                super().sendRequest(**{**request, 'eventQueue':self.DataQueue})
        if useDefaultQueue:
            return

        tmpEvents = []        
        got_full_response = False
        response_count = 0
        for _ in range(timeout*10):
            evt = self.DataQueue.nextEvent(100)
            # if print_msg:
            #     for msg in evt:
            #         print(msg)
            if evt.eventType() == 10 and got_full_response:
                break
            if evt.eventType() == 5:
                response_count += 1
                if print_msg:
                    print('Finish',response_count)
                got_full_response = response_count == len(requests)
            
            if evt.eventType() != 10:
                tmpEvents.append(evt)
                if print_msg:
                    print(SyncSession.Event_dict[evt.eventType()], len(tmpEvents))   
        return tmpEvents
        # rtn = []
        # for evt in tmpEvents:
        #     for msg in evt:
        #         rtn.append(msg.toPy())
        # return rtn
    