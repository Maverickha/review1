// Lightweight GA loader controlled by /config.js
(function(){
  window.__APP_CONFIG__ = window.__APP_CONFIG__ || { GA_ID: '' };
  const gaId = window.__APP_CONFIG__.GA_ID;
  if (!gaId) {
    console.info('GA disabled: GA_ID not set');
    window.logEvent = function(){ /* noop */ };
    return;
  }
  const s = document.createElement('script');
  s.async = true;
  s.src = `https://www.googletagmanager.com/gtag/js?id=${encodeURIComponent(gaId)}`;
  document.head.appendChild(s);
  window.dataLayer = window.dataLayer || [];
  function gtag(){ dataLayer.push(arguments); }
  window.gtag = gtag;
  gtag('js', new Date());
  gtag('config', gaId);
  window.logEvent = function(component, action, params){ gtag('event', action, { component, ...params }); };
})();


