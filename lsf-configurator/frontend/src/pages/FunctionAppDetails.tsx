import React, { useState, useMemo } from "react";
import {
  Typography,
  Box,
  Grid,
  Button,
  Tabs,
  Tab,
  Paper,
  Chip,
  Divider,
  Stack,
  TextField,
} from "@mui/material";
import { useParams } from "react-router";
import FunctionCompositionCard from "../components/FunctionCompositionCard";
import {
  useFunctionAppById,
  useUpdateFunctionAppLatencyLimit,
} from "../hooks/functionAppsHooks";
import type {
  FunctionComposition,
  Component,
  ComponentLink,
} from "../models/models";
import { generateComponentColor } from "../helpers/utilities";
import { useDeleteFunctionComposition } from "../hooks/functionCompositionHooks";
import AddIcon from "@mui/icons-material/Add";
import CallGraphView from "../components/CallGraphView";
import FunctionCompositionAddModal from "../components/FunctionCompositionAddModal";
import LinkIcon from "@mui/icons-material/Link";
import MemoryIcon from "@mui/icons-material/Memory";
import TimerIcon from "@mui/icons-material/Timer";
import SpeedIcon from "@mui/icons-material/Speed";
import LayoutCandidatesView from "../components/LayoutCandidatesView";
import ResultsView from "../components/ResultsView";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import { IconButton, Tooltip } from "@mui/material";
import EditIcon from "@mui/icons-material/Edit";
import SaveIcon from "@mui/icons-material/Save";
import CancelIcon from "@mui/icons-material/Cancel";

const FunctionAppDetails: React.FC = () => {
  const { id } = useParams();
  const { data: app, isLoading, error } = useFunctionAppById(id ?? "");
  const { mutate: deleteComposition } = useDeleteFunctionComposition();
  const { mutate: updateLatencyLimit } = useUpdateFunctionAppLatencyLimit();

  const [editingLatency, setEditingLatency] = useState(false);
  const [latencyValue, setLatencyValue] = useState(app?.latencyLimit ?? 0);

  const [tabValue, setTabValue] = useState<"list" | "graph" | "results">(
    "list"
  );
  const [isAddModalOpen, setAddModalOpen] = useState(false);
  const [copied, setCopied] = useState(false);

  const allDeployments = useMemo(
    () =>
      app?.compositions?.flatMap((composition) => composition.deployments) ??
      [],
    [app]
  );

  const namespace = useMemo(
    () => allDeployments[0]?.namespace ?? "",
    [allDeployments]
  );

  // Compute the app URL
  const appUrl = useMemo(() => {
    if (!app?.id || !namespace) return "";
    return `app-${app.id}.${namespace}.127.0.0.1.sslip.io`;
  }, [app, namespace]);

  const handleAddComposition = () => {
    setAddModalOpen(true);
  };

  const handleCloseAddModal = () => {
    setAddModalOpen(false);
  };

  const handleTabChange = (
    _: React.SyntheticEvent,
    newValue: "list" | "graph" | "results"
  ) => {
    setTabValue(newValue);
  };

  const handleCopyAppUrl = () => {
    if (!appUrl) return;
    navigator.clipboard.writeText(appUrl);
    setCopied(true);
    setTimeout(() => setCopied(false), 1200);
  };

  if (isLoading) {
    return (
      <Box
        sx={{
          height: "100vh",
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
        }}
      >
        <Typography variant="h6" align="center">
          Loading...
        </Typography>
      </Box>
    );
  }

  if (error) {
    return (
      <Box
        sx={{
          height: "100vh",
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
        }}
      >
        <Typography variant="h6" align="center" color="error">
          Error: {error.message}
        </Typography>
      </Box>
    );
  }

  if (!app) {
    return (
      <Box
        sx={{
          height: "100vh",
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
        }}
      >
        <Typography variant="body1" align="center">
          Function app not found.
        </Typography>
      </Box>
    );
  }

  return (
    <>
      <Box my={4}>
        <Typography variant="h4" gutterBottom>
          {app.name}({app.id})
        </Typography>
        {/* App URL with copy button */}
        {appUrl && (
          <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 2 }}>
            <Typography
              variant="subtitle2"
              sx={{ fontWeight: 700, color: "#388e3c" }}
            >
              URL:
            </Typography>
            <Typography
              variant="body2"
              sx={{
                fontFamily: "monospace",
                color: "#333",
                whiteSpace: "nowrap",
                overflow: "auto",
                flex: "none",
              }}
            >
              {appUrl}
              <Tooltip title={copied ? "Copied!" : "Copy"}>
                <IconButton
                  size="small"
                  onClick={handleCopyAppUrl}
                  sx={{ ml: 0.5 }}
                >
                  <ContentCopyIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            </Typography>
          </Stack>
        )}

        {/* Latency Limit */}
        <Box mb={3} display="flex" alignItems="center" gap={2}>
          <SpeedIcon color="primary" />
          <Typography variant="subtitle1" color="text.secondary">
            Latency Limit:
          </Typography>
          {editingLatency ? (
            <>
              <TextField
                size="small"
                type="number"
                value={latencyValue}
                onChange={(e) => setLatencyValue(Number(e.target.value))}
                sx={{ width: 100 }}
                inputProps={{ min: 1 }}
              />
              <IconButton
                color="primary"
                onClick={() => {
                  if (app?.id && latencyValue > 0) {
                    updateLatencyLimit({
                      id: app.id,
                      latencyLimit: latencyValue,
                    });
                    setEditingLatency(false);
                  }
                }}
                aria-label="Save"
              >
                <SaveIcon />
              </IconButton>
              <IconButton
                color="inherit"
                onClick={() => {
                  setLatencyValue(app.latencyLimit ?? 0);
                  setEditingLatency(false);
                }}
                aria-label="Cancel"
              >
                <CancelIcon />
              </IconButton>
            </>
          ) : (
            <>
              <Chip
                label={`${app.latencyLimit ?? "N/A"} ms`}
                color="primary"
                sx={{ fontWeight: "bold", fontSize: 16 }}
              />
              <IconButton
                size="small"
                onClick={() => setEditingLatency(true)}
                aria-label="Edit"
              >
                <EditIcon />
              </IconButton>
            </>
          )}
        </Box>

        {/* Components section */}
        <Typography variant="h5" gutterBottom>
          Components
        </Typography>
        <Grid container spacing={2} mb={4}>
          {app.components?.map((component: Component) => (
            <Grid key={component.name} size={{ xs: 12, sm: 6, md: 4 }}>
              <Paper
                elevation={3}
                sx={{
                  backgroundColor: generateComponentColor(
                    component.name,
                    65,
                    90
                  ),
                  border: `2px solid ${generateComponentColor(component.name)}`,
                  borderRadius: 2,
                  padding: 2,
                  textAlign: "center",
                  boxShadow: 2,
                  minHeight: 120,
                  display: "flex",
                  flexDirection: "column",
                  alignItems: "center",
                  gap: 1,
                }}
              >
                <Typography variant="h6" sx={{ fontWeight: 600 }}>
                  {component.name}
                </Typography>
                <Stack
                  direction="row"
                  spacing={1}
                  alignItems="center"
                  justifyContent="center"
                  mt={1}
                >
                  <Chip
                    icon={<MemoryIcon />}
                    label={`${component.memory} MB`}
                    size="small"
                    color="secondary"
                    sx={{ fontWeight: 500 }}
                  />
                  <Chip
                    icon={<TimerIcon />}
                    label={`${component.runtime} ms`}
                    size="small"
                    color="info"
                    sx={{ fontWeight: 500 }}
                  />
                </Stack>
              </Paper>
            </Grid>
          ))}
        </Grid>

        {/* Links section */}
        <Typography variant="h5" gutterBottom mt={4}>
          Links
        </Typography>
        <Grid container spacing={2} mb={4}>
          {app.links && app.links.length > 0 ? (
            app.links.map((link: ComponentLink, idx: number) => (
              <Grid size={{ xs: 12, sm: 6, md: 4 }} key={idx}>
                <Paper
                  elevation={2}
                  sx={{
                    borderRadius: 2,
                    padding: 2,
                    display: "flex",
                    alignItems: "center",
                    gap: 2,
                    background: "#f5faff",
                  }}
                >
                  <LinkIcon color="primary" />
                  <Box>
                    <Typography variant="subtitle1" sx={{ fontWeight: 500 }}>
                      {link.from} <span style={{ color: "#888" }}>→</span>{" "}
                      {link.to}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Invocation Rate:{" "}
                      <b>
                        {link.invocationRate?.min ?? "?"} -{" "}
                        {link.invocationRate?.max ?? "?"} /s
                      </b>
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      Data Delay: <b>{link.dataDelay ?? "?"} ms</b>
                    </Typography>
                  </Box>
                </Paper>
              </Grid>
            ))
          ) : (
            <Grid size={{ xs: 12 }}>
              <Typography variant="body2" color="text.secondary">
                No links defined.
              </Typography>
            </Grid>
          )}
        </Grid>

        {/* Layout Candidates Visualization */}
        <Box mb={4}>
          <LayoutCandidatesView
            layoutCandidates={app.layoutCandidates ?? {}}
            activeLayoutId={app.activeLayoutKey}
          />
        </Box>

        <Divider sx={{ my: 4 }} />

        {/* Tabs for compositions */}
        <Tabs value={tabValue} onChange={handleTabChange} sx={{ mb: 2 }}>
          <Tab label="List View" value="list" />
          <Tab label="Call Graph View" value="graph" />
          <Tab label="Results" value="results" />
        </Tabs>

        {tabValue === "list" && (
          <Grid container spacing={4}>
            {app.compositions?.map((composition: FunctionComposition) => (
              <Grid key={composition.id} size={{ xs: 12, sm: 6, md: 4 }}>
                <FunctionCompositionCard
                  composition={composition}
                  onDelete={deleteComposition}
                  allDeployments={allDeployments}
                />
              </Grid>
            ))}
            <Grid size={{ xs: 12, sm: 6, md: 4 }}>
              <Box
                sx={{
                  display: "flex",
                  justifyContent: "center",
                  alignItems: "center",
                  height: "100%",
                  border: "1px dashed #b2ebf2",
                  borderRadius: 2,
                  padding: 2,
                  boxShadow: 2,
                  cursor: "pointer",
                  minHeight: "600px",
                }}
              >
                <Button
                  variant="outlined"
                  startIcon={<AddIcon />}
                  onClick={handleAddComposition}
                >
                  Add Composition
                </Button>
              </Box>
            </Grid>
          </Grid>
        )}

        {tabValue === "graph" && <CallGraphView deployments={allDeployments} />}

        {tabValue === "results" && (
          <Box mt={2}>
            <ResultsView appId={app.id ?? ""} />
          </Box>
        )}
      </Box>
      <FunctionCompositionAddModal
        open={isAddModalOpen}
        onClose={handleCloseAddModal}
        appId={app.id ?? ""}
        appComponents={app.components?.map((comp) => comp.name) ?? []}
      />
    </>
  );
};

export default FunctionAppDetails;
