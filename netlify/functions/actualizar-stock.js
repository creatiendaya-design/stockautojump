// Node 18 en Netlify ya trae fetch global

// ========= utilidades =========
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function ymdInTZ(date, tz) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit'
  }).format(date); // YYYY-MM-DD
}
function summarize(obj, max = 5) {
  const out = { ...obj };
  for (const k of ['updated', 'skipped', 'debug']) {
    if (Array.isArray(out[k]) && out[k].length > max) {
      out[`${k}_sample`] = out[k].slice(0, max);
      out[`${k}_total`] = out[k].length;
      delete out[k];
    }
  }
  return out;
}
async function gql(store, token, query, variables = {}) {
  const version = process.env.SHOPIFY_API_VERSION || '2024-07';
  const r = await fetch(`${store}/admin/api/${version}/graphql.json`, {
    method: 'POST',
    headers: {
      'X-Shopify-Access-Token': token,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ query, variables })
  });
  const text = await r.text();
  if (!r.ok) throw new Error(`GraphQL ${r.status}: ${text}`);
  const json = JSON.parse(text);
  if (json.errors) throw new Error(`Errores de GraphQL: ${JSON.stringify(json.errors)}`);
  return json.data;
}
async function getAnyLocationIdREST(store, headers) {
  const version = process.env.SHOPIFY_API_VERSION || '2024-07';
  const r = await fetch(`${store}/admin/api/${version}/locations.json`, { headers });
  const tx = await r.text();
  if (!r.ok) throw new Error(`GET locations -> ${r.status}: ${tx}`);
  const { locations = [] } = JSON.parse(tx);
  if (!locations.length) throw new Error('No hay locations en la tienda');
  return locations[0].id;
}
async function putJSON(url, headers, body) {
  const r = await fetch(url, { method: 'PUT', headers, body: JSON.stringify(body) });
  const tx = await r.text();
  if (!r.ok) throw new Error(`PUT ${url} -> ${r.status}: ${tx}`);
  return JSON.parse(tx);
}
async function del(url, headers) {
  const r = await fetch(url, { method: 'DELETE', headers });
  if (!r.ok) {
    const tx = await r.text();
    throw new Error(`DELETE ${url} -> ${r.status}: ${tx}`);
  }
}

// ========= handler =========
exports.handler = async (event) => {
  try {
    const STORE   = process.env.SHOPIFY_STORE;        // https://tu-tienda.myshopify.com
    const TOKEN   = process.env.SHOPIFY_API_KEY;      // shpat_...
    const VERSION = process.env.SHOPIFY_API_VERSION || '2024-07';
    const DEFAULT_TZ = process.env.TIMEZONE || 'America/New_York';
    const FIXED_LOCATION_ID = process.env.LOCATION_ID; // opcional

    const missing = [];
    if (!STORE) missing.push('SHOPIFY_STORE');
    if (!TOKEN) missing.push('SHOPIFY_API_KEY');
    try { new URL(STORE); } catch { missing.push('SHOPIFY_STORE (URL inválida; incluye https://)'); }
    if (missing.length) {
      return { statusCode: 500, body: JSON.stringify({ error: 'Faltan variables', missing }) };
    }

    // Query params
    const qs = event.queryStringParameters || {};
    const productIdFilter = qs.productId ? Number(qs.productId) : null;
    const handleFilter    = qs.handle || null;
    const force           = qs.force === '1' || qs.force === 'true';
    const wantDebug       = qs.debug === '1' || qs.debug === 'true';
    const silent          = qs.silent === '1' || qs.silent === 'true';

    const headers = { 'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json' };

    const updated = [];
    const skipped = [];
    const debug   = [];

    // Location
    let locationId = FIXED_LOCATION_ID ? Number(FIXED_LOCATION_ID) : null;
    if (!locationId) locationId = await getAnyLocationIdREST(STORE, headers);

    // ========== selectores ==========
    // 1) Por ID puntual
    if (productIdFilter) {
      const data = await gql(STORE, TOKEN, `
        query($id: ID!) {
          product(id: $id) {
            id title handle
            variants(first: 1) {
              nodes {
                id
                legacyResourceId
                inventoryItem { id legacyResourceId tracked }
                variantStock: metafield(namespace: "custom", key: "stock_programado") { id value }
              }
            }
            metafield(namespace: "custom", key: "fecha_disponibilidad") { value }
            productStock: metafield(namespace: "custom", key: "stock_programado") { id value }
            metafieldTZ:   metafield(namespace: "custom", key: "timezone") { value }
          }
        }`, { id: `gid://shopify/Product/${productIdFilter}` });

      if (!data.product) {
        const payload = { message: 'Run OK', note: 'Producto no encontrado', updated, skipped, debug };
        if (silent) return { statusCode: 204, body: '' };
        return { statusCode: 200, headers:{'Cache-Control':'no-store'}, body: JSON.stringify(summarize(payload), null, 2) };
      }

      await processProducts({
        products: [data.product],
        STORE, VERSION, headers,
        defaultTZ: DEFAULT_TZ, force, locationId,
        updated, skipped, debug, wantDebug
      });

      const payload = {
        message: 'Run OK (single ID)',
        filters: { productIdFilter, force },
        counts: { updated: updated.length, skipped: skipped.length },
        updated, skipped, debug
      };
      if (silent) return { statusCode: 204, body: '' };
      return { statusCode: 200, headers:{'Cache-Control':'no-store'}, body: JSON.stringify(summarize(payload), null, 2) };
    }

    // 2) Por handle
    if (handleFilter) {
      const data = await gql(STORE, TOKEN, `
        query($query:String!) {
          products(first: 1, query: $query) {
            nodes {
              id title handle
              variants(first: 1) {
                nodes {
                  id
                  legacyResourceId
                  inventoryItem { id legacyResourceId tracked }
                  variantStock: metafield(namespace: "custom", key: "stock_programado") { id value }
                }
              }
              metafield(namespace: "custom", key: "fecha_disponibilidad") { value }
              productStock: metafield(namespace: "custom", key: "stock_programado") { id value }
              metafieldTZ:   metafield(namespace: "custom", key: "timezone") { value }
            }
          }
        }`, { query: `handle:${handleFilter}` });

      const products = data?.products?.nodes || [];

      await processProducts({
        products,
        STORE, VERSION, headers,
        defaultTZ: DEFAULT_TZ, force, locationId,
        updated, skipped, debug, wantDebug
      });

      const payload = {
        message: 'Run OK (handle)',
        filters: { handleFilter, force },
        counts: { updated: updated.length, skipped: skipped.length },
        updated, skipped, debug
      };
      if (silent) return { statusCode: 204, body: '' };
      return { statusCode: 200, headers:{'Cache-Control':'no-store'}, body: JSON.stringify(summarize(payload), null, 2) };
    }

    // 3) MASIVO: SOLO productos con ambos metafields a nivel producto
    //  (igual, si una variante tiene stock_programado lo usaremos prioritariamente)
    let hasNextPage = true;
    let cursor = null;
    let totalFetched = 0;

    while (hasNextPage) {
      const data = await gql(STORE, TOKEN, `
        query($after:String) {
          products(
            first: 100,
            after: $after,
            query: "metafield:custom.fecha_disponibilidad:* AND metafield:custom.stock_programado:*"
          ) {
            pageInfo { hasNextPage endCursor }
            nodes {
              id title handle
              variants(first: 1) {
                nodes {
                  id
                  legacyResourceId
                  inventoryItem { id legacyResourceId tracked }
                  variantStock: metafield(namespace: "custom", key: "stock_programado") { id value }
                }
              }
              metafield(namespace: "custom", key: "fecha_disponibilidad") { value }
              productStock: metafield(namespace: "custom", key: "stock_programado") { id value }
              metafieldTZ:   metafield(namespace: "custom", key: "timezone") { value }
            }
          }
        }`, { after: cursor });

      const products = data?.products?.nodes || [];
      totalFetched += products.length;

      await processProducts({
        products,
        STORE, VERSION, headers,
        defaultTZ: DEFAULT_TZ, force, locationId,
        updated, skipped, debug, wantDebug
      });

      hasNextPage = data?.products?.pageInfo?.hasNextPage || false;
      cursor      = data?.products?.pageInfo?.endCursor || null;

      await sleep(400); // pacing GraphQL
    }

    const payload = {
      message: 'Run OK (mass)',
      counts: { fetchedWithMetafields: totalFetched, updated: updated.length, skipped: skipped.length },
      updated, skipped, debug
    };
    if (silent) return { statusCode: 204, body: '' };
    return { statusCode: 200, headers:{'Cache-Control':'no-store'}, body: JSON.stringify(summarize(payload), null, 2) };

  } catch (err) {
    // En modo silent, no devuelvas cuerpo grande
    const qs = event?.queryStringParameters || {};
    if (qs.silent === '1' || qs.silent === 'true') {
      console.error('Error actualizar-stock:', err?.message || err);
      return { statusCode: 204, body: '' };
    }
    return { statusCode: 500, body: JSON.stringify({ error: err?.message || String(err) }) };
  }
};

// ========= procesador =========
async function processProducts({
  products, STORE, VERSION, headers, defaultTZ, force, locationId,
  updated, skipped, debug, wantDebug
}) {
  for (const p of products) {
    const fecha = p?.metafield?.value ? String(p.metafield.value).slice(0, 10) : null;
    const productStockId  = p?.productStock?.id || null;
    const productStockVal = p?.productStock?.value ? parseInt(p.productStock.value, 10) : 0;

    const productTZ = (p?.metafieldTZ?.value || defaultTZ).trim();
    const todayLocal = ymdInTZ(new Date(), productTZ);

    const v = p?.variants?.nodes?.[0];
    if (wantDebug) {
      debug.push({
        productId: p.id, handle: p.handle, fecha, productStockVal,
        productTZ, todayLocal,
        variantId: v?.legacyResourceId,
        variantItemId: v?.inventoryItem?.legacyResourceId,
        variantTracked: v?.inventoryItem?.tracked,
        variantStockVal: v?.variantStock?.value ? parseInt(v.variantStock.value, 10) : 0
      });
    }

    if (!force && (!fecha || fecha !== todayLocal)) {
      skipped.push({ productId: p.id, reason: 'date_mismatch_or_empty', fecha, todayLocal });
      continue;
    }
    if (!v) {
      skipped.push({ productId: p.id, reason: 'no_variant' });
      continue;
    }

    // Prioridad: stock programado en VARIANTE, si existe y >0; si no, PRODUCTO
    const variantStockId  = v?.variantStock?.id || null;
    const variantStockVal = v?.variantStock?.value ? parseInt(v.variantStock.value, 10) : 0;

    const stockToSet = variantStockVal > 0 ? variantStockVal : productStockVal;
    if (!stockToSet || stockToSet < 0) {
      skipped.push({ productId: p.id, variantId: v.legacyResourceId, reason: 'invalid_or_zero_stock', stockToSet });
      continue;
    }

    // Asegurar que el inventory item esté "tracked"
    if (v.inventoryItem && v.inventoryItem.tracked === false) {
      try {
        await putJSON(`${STORE}/admin/api/${VERSION}/inventory_items/${v.inventoryItem.legacyResourceId}.json`, headers, {
          inventory_item: { id: v.inventoryItem.legacyResourceId, tracked: true }
        });
      } catch (e) {
        skipped.push({ productId: p.id, variantId: v.legacyResourceId, reason: 'enable_tracked_failed', error: e.message });
        await sleep(700);
        continue;
      }
    }

    // Setear inventario en la location
    try {
      const r = await fetch(`${STORE}/admin/api/${VERSION}/inventory_levels/set.json`, {
        method: 'POST',
        headers,
        body: JSON.stringify({
          location_id: locationId,
          inventory_item_id: v.inventoryItem.legacyResourceId,
          available: stockToSet
        })
      });
      const tx = await r.text();
      if (!r.ok) throw new Error(`SET inventory -> ${r.status}: ${tx}`);

      // === Borrar metafields de stock_programado (variante y producto) ===
      // Borra SI existen; así evitas re-aplicar en siguientes ejecuciones.
      try {
        if (variantStockId) {
          await del(`${STORE}/admin/api/${VERSION}/metafields/${variantStockId}.json`, headers);
        }
      } catch (e) {
        // No es crítico para detener el flujo; solo registrar
        console.warn('DELETE variant stock_programado failed:', e.message);
      }
      try {
        if (productStockId) {
          await del(`${STORE}/admin/api/${VERSION}/metafields/${productStockId}.json`, headers);
        }
      } catch (e) {
        console.warn('DELETE product stock_programado failed:', e.message);
      }

      updated.push({
        productId: p.id,
        handle: p.handle,
        variantId: v.legacyResourceId,
        inventoryItemId: v.inventoryItem.legacyResourceId,
        locationId,
        setTo: stockToSet,
        clearedVariantStockMeta: Boolean(variantStockId),
        clearedProductStockMeta: Boolean(productStockId),
        fechaAplicada: force ? '(FORCED)' : todayInTZ(todayLocal)
      });
    } catch (e) {
      skipped.push({ productId: p.id, variantId: v.legacyResourceId, reason: 'inventory_set_failed', error: e.message });
    }

    await sleep(300); // pacing REST
  }
}

// helper para uniformidad del campo (por si agregas más adelante hora)
function todayInTZ(ymd) { return ymd; }
