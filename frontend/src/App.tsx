import { useState, useEffect } from "react";
import "@awesome.me/webawesome/dist/components/button/button.js";
import "@awesome.me/webawesome/dist/components/callout/callout.js";
import "@awesome.me/webawesome/dist/components/dialog/dialog.js";
import "@awesome.me/webawesome/dist/components/icon/icon.js";
import { Layout } from "./components/Layout";
import { WebsiteForm } from "./components/websites/WebsiteForm";
import { WebsiteList } from "./components/websites/WebsiteList";
import { Api, Website } from "./api/Api";

const api = new Api();

export function App() {
  const [sites, setSites] = useState<Website[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [editIndex, setEditIndex] = useState<number | null>(null);
  const [errorBanner, setErrorBanner] = useState<string | null>(null);

  useEffect(() => {
    api.publicApi.websitesIndex().then((response) => setSites(response.data));

    const ws = new WebSocket(`ws://${window.location.host}/ws/default`);
    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === "error") {
          if (msg.error?.RegionNotBound) {
            setErrorBanner(msg.error.RegionNotBound);
          } else if (msg.error?.GrpcUnavailable) {
            setErrorBanner(msg.error.GrpcUnavailable);
          }
        }
      } catch {
        // ignore malformed messages
      }
    };
    return () => ws.close();
  }, []);

  function handleAdd(site: { name: string; description: string }) {
    api.publicApi.createWebsite(site).then((response) => {
      setSites((prev) => [...prev, response.data]);
      setShowForm(false);
    });
  }

  function handleEdit(site: { name: string; description: string }) {
    setSites((prev) => prev.map((s, i) => (i === editIndex ? site : s)));
    setEditIndex(null);
  }

  const editingSite = editIndex !== null ? sites[editIndex] : undefined;

  return (
    <Layout
      title="Websites"
      headerActions={
        <wa-button variant="brand" onClick={() => setShowForm(true)}>
          Add Website
        </wa-button>
      }
      footer={<p>Footer content goes here.</p>}
    >
      {errorBanner && (
        <wa-callout variant="danger" appearance="filled">
          <wa-icon slot="icon" name="exclamation-octagon"></wa-icon>
          {errorBanner}
          <wa-button slot="footer" variant="neutral" size="small" onClick={() => setErrorBanner(null)}>
            Dismiss
          </wa-button>
        </wa-callout>
      )}

      <WebsiteList sites={sites} onEdit={setEditIndex} />

      <wa-dialog label="Add a New Website" open={showForm || undefined} onwa-after-hide={() => setShowForm(false)}>
        <WebsiteForm onSubmit={handleAdd} onCancel={() => setShowForm(false)} />
      </wa-dialog>

      <wa-dialog label="Edit Website" open={editIndex !== null || undefined} onwa-after-hide={() => setEditIndex(null)}>
        {editingSite && <WebsiteForm onSubmit={handleEdit} onCancel={() => setEditIndex(null)} initialValues={editingSite} submitLabel="Save Changes" />}
      </wa-dialog>
    </Layout>
  );
}
