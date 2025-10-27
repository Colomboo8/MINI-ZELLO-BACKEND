import express from "express"
import { WebSocketServer } from "ws"
import { createServer } from "http"
import cors from "cors"
import { v4 as uuidv4 } from "uuid"

const app = express()
const server = createServer(app)
const wss = new WebSocketServer({ server })

app.use(
  cors({
    origin: "*",
    credentials: true,
  }),
)
app.use(express.json())

const rooms = new Map()
const clients = new Map()

class Room {
  constructor(token) {
    this.token = token
    this.clients = new Set()
    this.createdAt = Date.now()
  }

  addClient(clientId) {
    this.clients.add(clientId)
    console.log(`[Server] Client ${clientId} joined room ${this.token}. Total clients: ${this.clients.size}`)
  }

  removeClient(clientId) {
    this.clients.delete(clientId)
    console.log(`[Server] Client ${clientId} left room ${this.token}. Remaining clients: ${this.clients.size}`)

    if (this.clients.size === 0) {
      console.log(`[Server] Room ${this.token} is empty, will be cleaned up`)
    }
  }

  broadcast(message, excludeClientId = null) {
    let sentCount = 0
    this.clients.forEach((clientId) => {
      if (clientId !== excludeClientId) {
        const client = clients.get(clientId)
        if (client && client.ws.readyState === 1) {
          client.ws.send(JSON.stringify(message))
          sentCount++
        }
      }
    })
    return sentCount
  }
}

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`)
  const token = url.searchParams.get("token")

  console.log(`[Server] ========== NEW CONNECTION ==========`)
  console.log(`[Server] Request URL: ${req.url}`)
  console.log(`[Server] Extracted token: ${token}`)
  console.log(`[Server] Current rooms:`, Array.from(rooms.keys()))

  if (!token) {
    console.log("[Server] Connection rejected: No token provided")
    ws.close(1008, "Token required")
    return
  }

  const clientId = uuidv4()
  console.log(`[Server] Assigned client ID: ${clientId}`)

  clients.set(clientId, { ws, token, clientId })

  let room = rooms.get(token)
  if (!room) {
    room = new Room(token)
    rooms.set(token, room)
    console.log(`[Server] Created new room with token: ${token}`)
  } else {
    console.log(`[Server] Joining existing room with token: ${token}`)
    console.log(`[Server] Room currently has ${room.clients.size} clients`)
  }

  room.addClient(clientId)

  console.log(`[Server] All clients in room ${token}:`, Array.from(room.clients))

  ws.send(
    JSON.stringify({
      type: "connected",
      clientId,
      roomToken: token,
      peersCount: room.clients.size - 1,
    }),
  )

  const broadcastResult = room.broadcast(
    {
      type: "peer_joined",
      clientId,
      peersCount: room.clients.size,
    },
    clientId,
  )
  console.log(`[Server] Broadcasted peer_joined to ${broadcastResult} clients`)

  ws.on("message", (data) => {
    try {
      const message = JSON.parse(data.toString())
      console.log(`[Server] Message from ${clientId}: ${message.type}`)

      switch (message.type) {
        case "audio":
          const sentTo = room.broadcast(
            {
              type: "audio",
              data: message.data,
              from: clientId,
            },
            clientId,
          )
          console.log(`[Server] Audio broadcasted to ${sentTo} clients`)
          break

        case "user_speaking":
          room.broadcast(
            {
              type: "user_speaking",
              clientId,
            },
            clientId,
          )
          break

        case "user_stopped":
          room.broadcast(
            {
              type: "user_stopped",
              clientId,
            },
            clientId,
          )
          break

        default:
          console.log(`[Server] Unknown message type: ${message.type}`)
      }
    } catch (error) {
      console.log("[Server] Error processing message:", error)
    }
  })

  ws.on("close", () => {
    console.log(`[Server] ========== CLIENT DISCONNECTED ==========`)
    console.log(`[Server] Client ${clientId} disconnected from room ${token}`)

    room.removeClient(clientId)
    clients.delete(clientId)

    room.broadcast({
      type: "peer_left",
      clientId,
      peersCount: room.clients.size,
    })

    if (room.clients.size === 0) {
      rooms.delete(token)
      console.log(`[Server] Room ${token} deleted (empty)`)
    } else {
      console.log(`[Server] Room ${token} still has ${room.clients.size} clients`)
    }
  })

  ws.on("error", (error) => {
    console.log(`[Server] WebSocket error for client ${clientId}:`, error)
  })
})

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    rooms: rooms.size,
    clients: clients.size,
    uptime: process.uptime(),
  })
})

app.get("/rooms", (req, res) => {
  const roomsData = Array.from(rooms.entries()).map(([token, room]) => ({
    token,
    clients: room.clients.size,
    createdAt: room.createdAt,
  }))

  res.json({
    total: rooms.size,
    rooms: roomsData,
  })
})

app.post("/validate-token", (req, res) => {
  const { token } = req.body

  if (!token) {
    return res.status(400).json({ valid: false, error: "Token required" })
  }

  const room = rooms.get(token)

  res.json({
    valid: true,
    exists: !!room,
    clients: room ? room.clients.size : 0,
  })
})

const PORT = process.env.PORT || 3001
const HOST = process.env.HOST || "0.0.0.0"

server.listen(PORT, HOST, () => {
  console.log(`[Server] Voice chat server running on port ${PORT}`)
  console.log(`[Server] WebSocket endpoint: ws://${HOST}:${PORT}`)
  console.log(`[Server] HTTP endpoint: http://${HOST}:${PORT}`)

  if (process.env.RAILWAY_ENVIRONMENT || process.env.RENDER) {
    console.log(`\n[Server] Running in CLOUD mode`)
    console.log(`[Server] Your server is accessible from anywhere!`)
  } else {
    console.log(`\n[Server] Running in LOCAL mode`)
    console.log(`[Server] IMPORTANT: Update your frontend config with your computer's IP address`)
    console.log(`[Server] To find your IP:`)
    console.log(`[Server]   Windows: ipconfig (look for IPv4 Address)`)
    console.log(`[Server]   Mac/Linux: ifconfig or ip addr (look for inet)`)
    console.log(`[Server] Example: ws://192.168.1.5:${PORT}\n`)
    console.log(`[Server] FIREWALL WARNING:`)
    console.log(`[Server]   If connection fails, temporarily disable Windows Defender Firewall`)
    console.log(`[Server]   Or add an inbound rule for port ${PORT}`)
    console.log(`[Server]   Settings > Windows Security > Firewall > Allow an app\n`)
  }
})

setInterval(
  () => {
    const now = Date.now()
    const oneHour = 60 * 60 * 1000

    rooms.forEach((room, token) => {
      if (room.clients.size === 0 && now - room.createdAt > oneHour) {
        rooms.delete(token)
        console.log(`[Server] Cleaned up inactive room: ${token}`)
      }
    })
  },
  5 * 60 * 1000,
)
