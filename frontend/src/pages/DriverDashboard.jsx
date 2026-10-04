import React, { useState, useEffect, useRef } from 'react';
import { MapContainer, TileLayer, CircleMarker, Polyline, Popup } from 'react-leaflet';
import SockJS from 'sockjs-client';
import { Stomp } from '@stomp/stompjs';
import axios from 'axios';

const API_BASE = 'http://localhost:8080/api';

const congestionColor = (score) => {
  if (score >= 0.75) return '#ef4444'; // red
  if (score >= 0.5) return '#f97316';  // orange
  if (score >= 0.25) return '#eab308'; // yellow
  return '#22c55e';                    // green
};

const DriverDashboard = () => {
  const [roadblocks, setRoadblocks] = useState([]);
  const [congestionEdges, setCongestionEdges] = useState([]);
  const [baseRoute, setBaseRoute] = useState(null);       // fast preview, shown instantly
  const [finalRoute, setFinalRoute] = useState(null);      // AI-arbitrated, replaces preview
  const [startQuery, setStartQuery] = useState('');
  const [destQuery, setDestQuery] = useState('');
  const [xai, setXai] = useState('Awaiting routing request...');
  const [isRouting, setIsRouting] = useState(false);
  const [stage, setStage] = useState('idle'); // idle | fast | arbitrating | done
  const [graphReady, setGraphReady] = useState(false);

  const pendingRequestId = useRef(null);
  const stompClientRef = useRef(null);

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
      .then(res => setRoadblocks(res.data.roadblocks || []));

    const pollCongestion = () => {
      axios.get(`${API_BASE}/congestion`)
        .then(res => setCongestionEdges(res.data.edges || []))
        .catch(() => {});
    };
    pollCongestion();
    const congestionInterval = setInterval(pollCongestion, 12000);

    const socket = new SockJS('http://localhost:8080/ws');
    const stompClient = Stomp.over(socket);
    stompClient.debug = () => {};
    stompClientRef.current = stompClient;

    stompClient.connect({}, () => {
      stompClient.subscribe('/topic/traffic', (message) => {
        const event = JSON.parse(message.body);
        if (event.event_type === 'ROADBLOCK_INJECTED') {
          setRoadblocks(prev => [...prev, event]);
        } else if (event.event_type === 'GRID_RESET') {
          setRoadblocks([]);
          setBaseRoute(null);
          setFinalRoute(null);
          setStage('idle');
          setXai('Grid cleared. Awaiting new routing request...');
        }
      });

      // Distinct topic from ambient congestion/roadblock events (Section 5) -
      // only route-completion payloads land here, matched by request_id.
      stompClient.subscribe('/topic/route-updates', (message) => {
        const decision = JSON.parse(message.body);
        if (decision.request_id !== pendingRequestId.current) return; // stale/other client's request
        setFinalRoute(decision.coordinates);
        setXai(decision.xai_explanation);
        setStage('done');
        setIsRouting(false);
      });
    });

    return () => {
      clearInterval(congestionInterval);
      if (stompClient.connected) stompClient.disconnect();
    };
  }, []);

  const requestRoute = async (e) => {
    e.preventDefault();
    setIsRouting(true);
    setStage('fast');
    setFinalRoute(null);
    setXai('Calculating fast preview route...');

    try {
      const res = await axios.post(`${API_BASE}/route`, {
        start_location: startQuery,
        destination_location: destQuery,
        emergency_type: 'Ambulance',
      });
      // The REST call itself already returns the AI-arbitrated result in
      // this implementation (single round trip). We still show the base
      // candidate instantly, then swap to the arbitrated one, so the UI
      // matches the two-stage behavior even though both arrive together.
      pendingRequestId.current = res.data.request_id;
      setBaseRoute(res.data.base_coordinates);
      setStage('arbitrating');
      setXai('AI arbiter is weighing candidate routes against live congestion...');

      // Small delay so the "fast route" -> "AI route" transition is visible,
      // then apply the arbitrated result directly (also broadcast via
      // /topic/route-updates for any other connected driver screens).
      setTimeout(() => {
        setFinalRoute(res.data.coordinates);
        setXai(res.data.xai_explanation);
        setStage('done');
        setIsRouting(false);
      }, 600);
    } catch (error) {
      setXai('Failed to calculate route. Ensure locations are within the West Hyderabad service area.');
      setIsRouting(false);
      setStage('idle');
    }
  };

  const activeRoute = finalRoute || baseRoute;

  return (
    <div className="flex flex-col h-screen bg-gray-900 text-white">
      {/* Banner in normal flow */}
      {!graphReady && (
        <div className="w-full bg-yellow-600 text-white text-center py-2 text-sm font-mono shrink-0 shadow-md">
          Graph loading from OSM — routing unavailable. Please wait...
        </div>
      )}

      {/* Main Content Area (Sidebar + Map) */}
      <div className="flex flex-1 overflow-hidden">
        {/* Left Sidebar */}
        <div className="w-1/3 p-6 border-r border-gray-700 flex flex-col gap-6 overflow-y-auto">
          <div>
            <h1 className="text-3xl font-bold text-blue-400 mb-2">Emergency Driver View</h1>
            <p className="text-sm text-gray-400">Unit: AMB-404 | Priority: Alpha</p>
          </div>

          <div className="bg-gray-800 p-4 rounded-lg shadow-lg">
            <form onSubmit={requestRoute} className="flex flex-col gap-4">
              <input
                type="text"
                placeholder="Start Location (e.g. Kukatpally)"
                className="p-3 bg-gray-700 rounded border border-gray-600 focus:outline-blue-500"
                value={startQuery} onChange={(e) => setStartQuery(e.target.value)} required
              />
              <input
                type="text"
                placeholder="Destination (e.g. Apollo Jubilee Hills)"
                className="p-3 bg-gray-700 rounded border border-gray-600 focus:outline-blue-500"
                value={destQuery} onChange={(e) => setDestQuery(e.target.value)} required
              />
              <button type="submit" disabled={isRouting} className={`font-bold py-3 rounded transition-colors ${isRouting ? 'bg-gray-600' : 'bg-blue-600 hover:bg-blue-700'}`}>
                {isRouting ? 'CALCULATING...' : 'REQUEST PRIORITY ROUTE'}
              </button>
            </form>
            {stage !== 'idle' && (
              <p className="text-xs mt-3 font-mono text-gray-400">
                Stage: {stage === 'fast' ? 'fast preview' : stage === 'arbitrating' ? 'AI arbitration in progress' : 'AI route confirmed'}
              </p>
            )}
          </div>

          <div className="bg-gray-800 p-4 rounded-lg border border-gray-700">
            <h2 className="text-sm font-semibold mb-2 text-gray-300">Congestion Legend</h2>
            <div className="flex flex-col gap-1 text-xs">
              <span><span className="inline-block w-3 h-3 mr-2 rounded-full" style={{ background: '#22c55e' }} />Light</span>
              <span><span className="inline-block w-3 h-3 mr-2 rounded-full" style={{ background: '#eab308' }} />Moderate</span>
              <span><span className="inline-block w-3 h-3 mr-2 rounded-full" style={{ background: '#f97316' }} />Heavy</span>
              <span><span className="inline-block w-3 h-3 mr-2 rounded-full" style={{ background: '#ef4444' }} />Severe</span>
            </div>
          </div>

          <div className="bg-gray-800 p-4 rounded-lg shadow-lg flex-grow border border-blue-900/50">
            <h2 className="text-xl font-semibold mb-2 text-blue-300">Explainable AI (XAI) Insight</h2>
            <div className="bg-gray-900 p-4 rounded h-full">
              <p className="text-sm font-mono leading-relaxed text-blue-100">{xai}</p>
            </div>
          </div>
        </div>

        {/* Right Map Container */}
        <div className="w-2/3 h-full relative">
          <MapContainer center={[17.4399, 78.3820]} zoom={12} className="h-full w-full">
            <TileLayer
              url="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}"
              attribution='Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ'
            />
            {congestionEdges.map((edge, idx) => (
              <Polyline
                key={`cong-${idx}`}
                positions={[[edge.u_lat, edge.u_lon], [edge.v_lat, edge.v_lon]]}
                pathOptions={{ color: congestionColor(edge.score), weight: 4, opacity: 0.6 }}
              />
            ))}
            {roadblocks.map((block, idx) => (
              <CircleMarker key={`block-${idx}`} center={[block.latitude, block.longitude]} pathOptions={{ color: 'red', fillColor: 'red', fillOpacity: 0.6 }} radius={9}>
                <Popup>{block.road_name || 'Roadblock'}</Popup>
              </CircleMarker>
            ))}
            {baseRoute && stage === 'arbitrating' && (
              <Polyline positions={baseRoute} pathOptions={{ color: '#64748b', weight: 4, opacity: 0.6, dashArray: '6 8' }} />
            )}
            {activeRoute && (
              <Polyline
                positions={activeRoute}
                pathOptions={{ color: finalRoute ? '#3b82f6' : '#64748b', weight: 6, opacity: 0.9 }}
              />
            )}
          </MapContainer>
        </div>
      </div>
    </div>
  );
};

export default DriverDashboard;