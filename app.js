document.body.insertAdjacentHTML("afterbegin","<div style='padding:8px;color:#6ee7ff'>JS running ✅</div>");
// Change this later to your real API URL (example: https://yourdomain.com/report/today)
const API_TODAY = null; // null = use mock data

const MOCK = {
  date: "2026-01-29",
  convergence: [
    { id:"c1", ticker:"NVDA", companyName:"NVIDIA", demBuyers:2, repBuyers:1, funds:["Qatar Investment Authority","Norges Bank"], lastFiledAt:"2026-01-29", strength:"VERY HIGH" },
    { id:"c2", ticker:"MSFT", companyName:"Microsoft", demBuyers:1, repBuyers:1, funds:["CPPIB"], lastFiledAt:"2026-01-29", strength:"HIGH" }
  ],
  bipartisanTickers: [
    { id:"b1", ticker:"LLY", companyName:"Eli Lilly", demBuyers:1, repBuyers:1, funds:[], lastFiledAt:"2026-01-29", strength:"MED" }
  ],
  fundSignals: [
    { id:"f1", ticker:"NVDA", companyName:"NVIDIA", signal:{ fundName:"Qatar Investment Authority", signalType:"13G_INCREASE", filedAt:"2026-01-29", ownershipChange:"5.2% → 6.1%" } },
    { id:"f2", ticker:"AAPL", companyName:"Apple", signal:{ fundName:"Norges Bank", signalType:"13F_INCREASE", filedAt:"2026-01-29", ownershipChange:null } }
  ]
};

function el(id){ return document.getElementById(id); }

function cardHTML(x){
  const funds = x.funds && x.funds.length ? `Funds: ${x.funds.slice(0,2).join(", ")}${x.funds.length>2 ? " +" + (x.funds.length-2) : ""}` : "";
  return `
    <div class="card">
      <div class="row">
        <div class="title">${x.ticker} · ${x.companyName}</div>
        <div class="badge">${x.strength || ""}</div>
      </div>
      <div class="meta">D buyers: ${x.demBuyers} | R buyers: ${x.repBuyers}</div>
      ${funds ? `<div class="meta">${funds}</div>` : ""}
      ${x.lastFiledAt ? `<div class="meta">Last filed: ${x.lastFiledAt}</div>` : ""}
    </div>
  `;
}

function fundRowHTML(r){
  return `
    <div class="card">
      <div class="row">
        <div class="title">${r.ticker} · ${r.companyName}</div>
        <div class="badge">${r.signal.signalType}</div>
      </div>
      <div class="meta">${r.signal.fundName} · Filed ${r.signal.filedAt}</div>
      ${r.signal.ownershipChange ? `<div class="meta">${r.signal.ownershipChange}</div>` : ""}
    </div>
  `;
}

async function load(){
  let data = MOCK;
  if (API_TODAY) {
    const res = await fetch(API_TODAY, { cache: "no-store" });
    data = await res.json();
  }

  el("reportDate").textContent = data.date || "";
  el("convergenceList").innerHTML = (data.convergence || []).map(cardHTML).join("");
  el("bipartisanList").innerHTML = (data.bipartisanTickers || []).map(cardHTML).join("");
  el("fundList").innerHTML = (data.fundSignals || []).map(fundRowHTML).join("");
}

el("refreshBtn").addEventListener("click", (e) => { e.preventDefault(); load(); });

load();
