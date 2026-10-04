import React, { useState, useEffect, useCallback } from 'react';
import { MapContainer, TileLayer, Popup, CircleMarker, Polyline, useMapEvents } from 'react-leaflet';
import SockJS from 'sockjs-client';
import { Stomp } from '@stomp/stompjs';
import axios from 'axios';
import L from 'leaflet';

delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
  iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
});

const API_BASE = 'http://localhost:8080/api';

const congestionColor = (score) => {
  if (score >= 0.75) return '#ef4444'; // red
  if (score >= 0.5) return '#f97316';  // orange
  if (score >= 0.25) return '#eab308'; // yellow
  return '#22c55e';                    // green
};

// Captures raw clicks on the map for click-to-block. Does NOT block
// immediately - the sidebar confirm step handles that (Section 4).
const ClickCapture = ({ onMapClick }) => {
  useMapEvents({
    click(e) {
      onMapClick(e.latlng);
    },
  });
  return null;
};

const AdminDashboard = () => {
  const [roadblocks, setRoadblocks] = useState([]);
  const [congestionEdges, setCongestionEdges] = useState([]);
  const [status, setStatus] = useState('Connected to Live Grid');
  const [pendingClick, setPendingClick] = useState(null); // {lat, lng} awaiting confirm
  const [graphReady, setGraphReady] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const pollStatus = () => {
      axios.get(`${API_BASE}/status`)
        .then(res => {
          if (cancelled) return;
          if (res.data.graph_ready) setGraphReady(true);
        })
        .catch(() => {});
    };
    pollStatus();
    const statusInterval = setInterval(pollStatus, 3000);
    return () => { cancelled = true; clearInterval(statusInterval); };
  }, []);

  useEffect(() => {
    axios.get(`${API_BASE}/roadblocks`)
      .then(res => setRoadblocks(res.data.roadblocks || []))
      .catch(() => console.error('Failed to load initial roadblocks'));

    const pollCongestion = () => {
      axios.get(`${API_BASE}/congestion`)
        .then(res => setCongestionEdges(res.data.edges || []))
        .catch(() => {});
    };
    pollCongestion();
    const interval = setInterval(pollCongestion, 12000);

    const socket = new SockJS('http://localhost:8080/ws');
    const stompClient = Stomp.over(socket);
    stompClient.debug = () => {};

    stompClient.connect({}, () => {
      stompClient.subscribe('/topic/traffic', (message) => {
        const event = JSON.parse(message.body);
        if (event.event_type === 'ROADBLOCK_INJECTED') {
          setRoadblocks(prev => [...prev, event]);
        } else if (event.event_type === 'GRID_RESET') {
          setRoadblocks([]);
        }
        // CONGESTION_UPDATE events also arrive on this topic, but the admin
        // map refreshes coloring from /api/congestion on a timer instead of
        // parsing the full per-edge payload client-side on every tick.
      });
    }, () => setStatus('Connection Lost. Reconnecting...'));

    return () => {
      clearInterval(interval);
      if (stompClient.connected) stompClient.disconnect();
    };
  }, []);

  const handleMapClick = useCallback((latlng) => {
    setPendingClick({ lat: latlng.lat, lng: latlng.lng });
    setStatus('Road segment selected. Confirm to add roadblock.');
  }, []);

  const confirmRoadblock = async () => {
    if (!pendingClick) return;
    setStatus('Injecting roadblock...');
    try {
      await axios.post(`${API_BASE}/roadblock`, {
        latitude: pendingClick.lat,
        longitude: pendingClick.lng,
        incident_type: 'Severe Collision',
        severity: 'High',
      });
      setStatus('Roadblock injected successfully.');
      setPendingClick(null);
    } catch (error) {
      setStatus('Error: Could not block that segment.');
    }
  };

  const cancelPending = () => {
    setPendingClick(null);
    setStatus('Selection cleared.');
  };

  const resetGrid = async () => {
    await axios.post(`${API_BASE}/reset`);
    setStatus('Grid reset complete.');
  };

  return (
    <div className="flex flex-col h-screen bg-gray-900 text-white">
      {/* Banner in the normal document flow (pushes content down) */}
      {!graphReady && (
        <div className="w-full bg-yellow-600 text-white text-center py-2 text-sm font-mono shrink-0 shadow-md">
          Graph loading from OSM — routing and roadblocks unavailable. Please wait...
        </div>
      )}

      {/* Main Content Area (Sidebar + Map) */}
      <div className="flex flex-1 overflow-hidden">
        {/* Left Sidebar */}
        <div className="w-1/3 p-6 border-r border-gray-700 flex flex-col gap-6 overflow-y-auto">
          <div>
            <h1 className="text-3xl font-bold text-red-500 mb-2">UrbanPulse Admin</h1>
            <p className="text-sm text-gray-400">Emergency Dispatch Simulator</p>
          </div>

          <div className="bg-gray-800 p-4 rounded-lg shadow-lg">
            <h2 className="text-xl font-semibold mb-2">Click-to-Block</h2>
            <p className="text-sm text-gray-400 mb-4">
              Click directly on a road segment on the map to select it, then confirm below.
            </p>

            {pendingClick ? (
              <div className="flex flex-col gap-3 bg-gray-900 p-3 rounded border border-yellow-600">
                <p className="text-sm text-yellow-300">
                  Segment selected near ({pendingClick.lat.toFixed(5)}, {pendingClick.lng.toFixed(5)})
                </p>
                <div className="flex gap-2">
                  <button onClick={confirmRoadblock} className="flex-1 bg-red-600 hover:bg-red-700 font-bold py-2 rounded transition-colors">
                    ADD ROADBLOCK
                  </button>
                  <button onClick={cancelPending} className="flex-1 bg-gray-700 hover:bg-gray-600 font-bold py-2 rounded transition-colors">
                    CANCEL
                  </button>
                </div>
              </div>
            ) : (
              <p className="text-sm font-mono text-gray-500">No segment selected.</p>
            )}
          </div>

          <button onClick={resetGrid} className="bg-gray-700 hover:bg-gray-600 text-white font-bold py-2 rounded transition-colors">
            Reset Entire Grid
          </button>

          <div className="bg-gray-800 p-4 rounded-lg border border-gray-700">
            <h2 className="text-sm font-semibold mb-2 text-gray-300">Congestion Legend</h2>
            <div className="flex flex-col gap-1 text-xs">
              <span><span className="inline-block w-3 h-3 mr-2 rounded-full" style={{ background: '#22c55e' }} />Light</span>
              <span><span className="inline-block w-3 h-3 mr-2 rounded-full" style={{ background: '#eab308' }} />Moderate</span>
              <span><span className="inline-block w-3 h-3 mr-2 rounded-full" style={{ background: '#f97316' }} />Heavy</span>
              <span><span className="inline-block w-3 h-3 mr-2 rounded-full" style={{ background: '#ef4444' }} />Severe</span>
            </div>
          </div>

          <div className="mt-auto p-4 bg-gray-800 rounded border border-gray-700">
            <p className="text-sm font-mono text-green-400">&gt; {status}</p>
          </div>
        </div>

        {/* Right Map Container */}
        <div className="w-2/3 h-full relative">
          <MapContainer center={[17.4399, 78.3820]} zoom={13} className="h-full w-full">
            <TileLayer
              url="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}"
              attribution='Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ'
            />
            <ClickCapture onMapClick={handleMapClick} />

            {congestionEdges.map((edge, idx) => (
              <Polyline
                key={`cong-${idx}`}
                positions={[[edge.u_lat, edge.u_lon], [edge.v_lat, edge.v_lon]]}
                pathOptions={{ color: congestionColor(edge.score), weight: 4, opacity: 0.7 }}
              />
            ))}

            {roadblocks.map((block, idx) => (
              <CircleMarker
                key={`block-${idx}`}
                center={[block.latitude, block.longitude]}
                pathOptions={{ color: 'red', fillColor: 'red', fillOpacity: 0.6 }}
                radius={9}
              >
                <Popup>{block.road_name || 'Roadblock'}</Popup>
              </CircleMarker>
            ))}

            {pendingClick && (
              <CircleMarker
                center={[pendingClick.lat, pendingClick.lng]}
                pathOptions={{ color: '#facc15', fillColor: '#facc15', fillOpacity: 0.5 }}
                radius={10}
              >
                <Popup>Pending confirmation</Popup>
              </CircleMarker>
            )}
          </MapContainer>
        </div>
      </div>
    </div>
  );
};

export default AdminDashboard;