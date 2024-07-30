---- MODULE walsync ----
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Writer, Conflictor, MaxFrameID

VARIABLES 
    clientDB,        \* Client database state
    clientWAL,       \* Client WAL frames
    clientCheckpoint,\* Client's last checkpoint frame ID
    serverDB,        \* Server database state
    serverWAL,       \* Server WAL frames
    serverCheckpoint,\* Server's last checkpoint frame ID
    messages         \* Messages in transit

vars == << clientDB, clientWAL, clientCheckpoint, 
          serverDB, serverWAL, serverCheckpoint, messages >>

Clients == {Writer, Conflictor}

Message == [type: {"FetchDatabase", "PullWAL", "PushWAL"}, 
            sender: Clients,
            payload: [clientId: Clients, 
                      baseFrameId: Nat,
                      frames: Seq(Nat),
                      lastCheckpointFrameId: Nat,
                      requestCheckpoint: BOOLEAN]]

Response == [type: {"DatabaseChunk", "PullWALResponse", "PushWALResponse"},
             receiver: Clients,
             payload: [status: {"SUCCESS", "CONFLICT", "ERROR", "NEED_FULL_SYNC"},
                       frames: Seq(Nat),
                       serverLastCheckpointFrameId: Nat,
                       performCheckpoint: BOOLEAN,
                       checkpointFrameId: Nat]]

TypeOK ==
    /\ clientDB \in [Clients -> Nat]
    /\ clientWAL \in [Clients -> Seq(Nat)]
    /\ clientCheckpoint \in [Clients -> Nat]
    /\ serverDB \in Nat
    /\ serverWAL \in Seq(Nat)
    /\ serverCheckpoint \in Nat
    /\ messages \subseteq (Message \union Response)

Init ==
    /\ clientDB = [c \in Clients |-> 0]
    /\ clientWAL = [c \in Clients |-> <<>>]
    /\ clientCheckpoint = [c \in Clients |-> 0]
    /\ serverDB = 0
    /\ serverWAL = <<>>
    /\ serverCheckpoint = 0
    /\ messages = {}

\* Helper function to get the last frame ID
LastFrameId(wal) == IF Len(wal) = 0 THEN 0 ELSE wal[Len(wal)]

ClientWrite ==
    /\ LET newFrame == LastFrameId(clientWAL[Writer]) + 1
       IN  /\ newFrame <= MaxFrameID
           /\ clientWAL' = [clientWAL EXCEPT ![Writer] = Append(@, newFrame)]
    /\ UNCHANGED << clientDB, clientCheckpoint, serverDB, serverWAL, serverCheckpoint, messages >>

RequestFetchDatabase(c) ==
    /\ messages' = messages \union {[type |-> "FetchDatabase", sender |-> c, 
                                     payload |-> [clientId |-> c]]}
    /\ UNCHANGED << clientDB, clientWAL, clientCheckpoint, serverDB, serverWAL, serverCheckpoint >>

RespondFetchDatabase ==
    \E m \in messages :
        /\ m.type = "FetchDatabase"
        /\ LET response == [type |-> "DatabaseChunk", 
                            receiver |-> m.sender,
                            payload |-> [status |-> "SUCCESS", 
                                         frames |-> <<>>,
                                         serverLastCheckpointFrameId |-> serverCheckpoint,
                                         performCheckpoint |-> FALSE,
                                         checkpointFrameId |-> 0]]
           IN  /\ messages' = (messages \ {m}) \union {response}
               /\ clientDB' = [clientDB EXCEPT ![m.sender] = serverDB]
    /\ UNCHANGED << clientWAL, clientCheckpoint, serverDB, serverWAL, serverCheckpoint >>

RequestPullWAL(c) ==
    /\ messages' = messages \union {[type |-> "PullWAL", sender |-> c, 
                                     payload |-> [clientId |-> c, 
                                                  lastCheckpointFrameId |-> clientCheckpoint[c]]]}
    /\ UNCHANGED << clientDB, clientWAL, clientCheckpoint, serverDB, serverWAL, serverCheckpoint >>

RespondPullWAL ==
    \E m \in messages :
        /\ m.type = "PullWAL"
        /\ LET newFrames == SubSeq(serverWAL, m.payload.lastCheckpointFrameId + 1, Len(serverWAL))
               response == [type |-> "PullWALResponse", 
                            receiver |-> m.sender,
                            payload |-> [status |-> "SUCCESS", 
                                         frames |-> newFrames,
                                         serverLastCheckpointFrameId |-> serverCheckpoint,
                                         performCheckpoint |-> FALSE,
                                         checkpointFrameId |-> 0]]
           IN  /\ messages' = (messages \ {m}) \union {response}
               /\ clientWAL' = [clientWAL EXCEPT ![m.sender] = @ \o newFrames]
    /\ UNCHANGED << clientDB, clientCheckpoint, serverDB, serverWAL, serverCheckpoint >>

RequestPushWAL(c) ==
    /\ LET request == [type |-> "PushWAL", 
                       sender |-> c,
                       payload |-> [clientId |-> c, 
                                    baseFrameId |-> LastFrameId(clientWAL[c]),
                                    frames |-> clientWAL[c],
                                    lastCheckpointFrameId |-> clientCheckpoint[c],
                                    requestCheckpoint |-> TRUE]]
       IN messages' = messages \union {request}
    /\ UNCHANGED << clientDB, clientWAL, clientCheckpoint, serverDB, serverWAL, serverCheckpoint >>

RespondPushWAL ==
    \E m \in messages :
        /\ m.type = "PushWAL"
        /\ LET doCheckpoint == \/ Len(serverWAL) > 2 * Len(m.payload.frames)
                               \/ m.payload.requestCheckpoint
               newServerWAL == IF /\ m.sender = Writer 
                                  /\ m.payload.baseFrameId = LastFrameId(serverWAL)
                               THEN serverWAL \o m.payload.frames
                               ELSE serverWAL
               newCheckpointId == IF doCheckpoint THEN LastFrameId(newServerWAL) ELSE serverCheckpoint
               response == [type |-> "PushWALResponse", 
                            receiver |-> m.sender,
                            payload |-> [status |-> IF /\ m.sender = Writer
                                                      /\ m.payload.baseFrameId = LastFrameId(serverWAL) 
                                                    THEN "SUCCESS" ELSE "CONFLICT",
                                         frames |-> IF /\ m.sender = Writer
                                                      /\ m.payload.baseFrameId = LastFrameId(serverWAL)
                                                    THEN <<>> ELSE serverWAL,
                                         serverLastCheckpointFrameId |-> newCheckpointId,
                                         performCheckpoint |-> doCheckpoint,
                                         checkpointFrameId |-> newCheckpointId]]
           IN  /\ messages' = (messages \ {m}) \union {response}
               /\ serverWAL' = newServerWAL
               /\ serverCheckpoint' = newCheckpointId
               /\ IF doCheckpoint 
                  THEN serverDB' = LastFrameId(newServerWAL)
                  ELSE UNCHANGED serverDB
    /\ UNCHANGED << clientDB, clientWAL, clientCheckpoint >>

HandlePushWALResponse ==
    \E m \in messages :
        /\ m.type = "PushWALResponse"
        /\ LET client == m.receiver
           IN  /\ IF m.payload.status = "SUCCESS"
                  THEN /\ IF m.payload.performCheckpoint
                          THEN /\ clientCheckpoint' = [clientCheckpoint EXCEPT ![client] = m.payload.checkpointFrameId]
                               /\ clientDB' = [clientDB EXCEPT ![client] = LastFrameId(clientWAL[client])]
                               /\ clientWAL' = [clientWAL EXCEPT ![client] = SubSeq(@, m.payload.checkpointFrameId + 1, Len(@))]
                          ELSE UNCHANGED << clientCheckpoint, clientDB, clientWAL >>
                  ELSE /\ clientWAL' = [clientWAL EXCEPT ![client] = m.payload.frames]
                       /\ UNCHANGED << clientCheckpoint, clientDB >>
               /\ messages' = messages \ {m}
    /\ UNCHANGED << serverDB, serverWAL, serverCheckpoint >>

Next ==
    \/ ClientWrite
    \/ \E c \in Clients : RequestFetchDatabase(c)
    \/ RespondFetchDatabase
    \/ \E c \in Clients : RequestPullWAL(c)
    \/ RespondPullWAL
    \/ \E c \in Clients : RequestPushWAL(c)
    \/ RespondPushWAL
    \/ HandlePushWALResponse

Spec == Init /\ [][Next]_vars

\* Invariant: Writer's WAL and remote WAL match
WriterWALMatchesServer ==
    \A i \in 1..Len(clientWAL[Writer]) :
        i <= Len(serverWAL) => clientWAL[Writer][i] = serverWAL[i]

\* Invariant: Remote WAL never has conflicting writes
NoConflictingWrites ==
    \A i, j \in 1..Len(serverWAL) :
        i # j => serverWAL[i] # serverWAL[j]

THEOREM Spec => [](TypeOK /\ WriterWALMatchesServer /\ NoConflictingWrites)

====