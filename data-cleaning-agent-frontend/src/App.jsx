import React, { useState, useEffect } from "react";
import styled from "styled-components";
import { FaBroom, FaEdit, FaEye, FaCheck, FaTimes } from "react-icons/fa";

// --- Styled Components (Refined for Modern Look) ---
const Wrapper = styled.div`
  min-height: 100vh;
  background: linear-gradient(135deg, #181e41 40%, #3f5efb 100%);
  padding: 2.5rem 0;
`;

const CleanBox = styled.div`
  background: rgba(39, 47, 89, 0.7);
  border-radius: 28px;
  padding: 2.5rem 2rem;
  box-shadow: 0 6px 48px 0 #3f5efb50;
  margin: 2.5rem auto 0 auto;
  max-width: 680px;
  border: 1px solid #383d5a;
`;

const SectionTitle = styled.h2`
  font-weight: 800;
  font-size: 2.2rem;
  letter-spacing: 0.01em;
  margin-bottom: 1.2rem;
  color: #ffe082;
  display: flex;
  align-items: center;
  gap: 1.1rem;
`;

const DataStatus = styled.div`
  background: linear-gradient(90deg, #2e3264 60%, #3f5efb22 100%);
  border-radius: 14px;
  padding: 1.4rem;
  margin-top: 1.3rem;
  box-shadow: 0 2px 16px #3f5efb18;

  h4 {
    color: #ffe082;
    margin: 0 0 0.75rem 0;
    font-weight: 700;
    font-size: 1.2rem;
  }
  p {
    color: #fff;
    margin: 0.15rem 0;
    font-size: 1rem;
  }
`;

const Option = styled.div`
  display: flex;
  align-items: center;
  margin: 1.1rem 0;
  label {
    font-size: 1.13rem;
    color: #e3e5ec;
    margin-left: 1rem;
    font-weight: 600;
    letter-spacing: 0.02em;
  }
  input[type="checkbox"] {
    accent-color: #fc466b;
    width: 1.25rem;
    height: 1.25rem;
    box-shadow: 0 0 2px #ffe082;
  }
`;

const NextBtn = styled.button`
  background: linear-gradient(90deg, #fc466b, #3f5efb 100%);
  border: none;
  padding: 1rem 2.8rem;
  border-radius: 36px;
  font-size: 1.18rem;
  font-weight: bold;
  color: #ffe082;
  margin-top: 2.3rem;
  cursor: pointer;
  box-shadow: 0 2px 14px #fc466b35;
  transition: 0.2s;
  &:hover {
    background: linear-gradient(90deg, #ffe082, #fc466b 100%);
    color: #3f5efb;
    box-shadow: 0 4px 16px #ffe08244;
  }
`;

const BackBtn = styled.button`
  background: linear-gradient(90deg, #3f3e55, #595e76);
  border: none;
  padding: 1rem 2.1rem;
  border-radius: 32px;
  font-size: 1.1rem;
  font-weight: bold;
  color: #fff;
  margin-top: 2.3rem;
  margin-right: 1.3rem;
  cursor: pointer;
  box-shadow: 0 2px 10px #33344c33;
  transition: 0.18s;
  &:hover {
    background: linear-gradient(90deg, #888, #aaa);
  }
`;

const FillMissingBtn = styled.button`
  background: linear-gradient(90deg, #3f5efb, #fc466b);
  border: none;
  padding: 0.8rem 1.7rem;
  border-radius: 22px;
  font-size: 1rem;
  font-weight: bold;
  color: #fff;
  margin-top: 1.2rem;
  cursor: pointer;
  box-shadow: 0 2px 10px #3f5efb44;
  display: inline-flex;
  align-items: center;
  gap: 0.7rem;
  transition: all 0.2s;
  &:hover {
    background: linear-gradient(90deg, #fc466b, #3f5efb);
    transform: scale(1.05);
  }
`;

const ButtonGroup = styled.div`
  display: flex;
  align-items: center;
  margin-top: 2.2rem;
  margin-bottom: 2rem;
`;

const ModalBackdrop = styled.div`
  position: fixed;
  top: 0; left: 0; width: 100vw; height: 100vh;
  background: rgba(19, 25, 39, 0.96);
  display: flex; justify-content: center; align-items: center;
  z-index: 1000;
  backdrop-filter: blur(6px);
`;

const ModalContent = styled.div`
  background: linear-gradient(135deg, #2c3e50, #242d3a 80%);
  border-radius: 28px;
  padding: 2.5rem 2rem;
  max-width: 96vw;
  max-height: 88vh;
  overflow-y: auto;
  box-shadow: 0 20px 64px #181e41ee;
  border: 1.5px solid #ffe08233;
  min-width: 640px;
`;

const ModalHeader = styled.div`
  display: flex; justify-content: space-between; align-items: center;
  border-bottom: 1px solid #ffe08233;
  margin-bottom: 1.5rem; padding-bottom: 1rem;
  h3 {
    color: #ffe082;
    margin: 0;
    font-size: 1.45rem;
    display: flex;
    align-items: center;
    gap: 0.6rem;
    font-weight: 700;
  }
`;

const CloseBtn = styled.button`
  background: linear-gradient(90deg, #e74c3c, #c0392b);
  border: none;
  color: #fff;
  padding: 0.7rem 1.4rem;
  border-radius: 14px;
  cursor: pointer;
  font-weight: bold;
  display: flex;
  align-items: center;
  gap: 0.4rem;
  font-size: 1.1rem;
  transition: 0.18s;
  &:hover {
    background: linear-gradient(90deg, #c0392b, #a93226);
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(231,76,60,0.3);
  }
`;

const MissingDataTable = styled.table`
  width: 100%;
  border-collapse: collapse;
  border-radius: 14px;
  overflow: hidden;
  box-shadow: 0 4px 15px #181e4140;
  th, td {
    padding: 1.1rem 0.7rem;
    text-align: left;
    border-bottom: 1px solid #34495e;
    font-size: 0.97rem;
  }
  th {
    background: linear-gradient(90deg, #34495e, #222b3a);
    color: #ffe082;
    font-weight: 700;
    position: sticky;
    top: 0;
    z-index: 10;
    letter-spacing: 0.5px;
    text-transform: uppercase;
  }
  td {
    background: rgba(52,152,219,0.11);
    color: #fff;
  }
  tr:hover td {
    background: rgba(52,152,219,0.19);
    transform: scale(1.01);
  }
`;

const MissingInput = styled.input`
  background: linear-gradient(135deg, #34495e, #232a37);
  border: 2px solid #3498db;
  color: #fff;
  padding: 0.7rem;
  border-radius: 10px;
  width: 100%;
  font-size: 1rem;
  &:focus {
    outline: none;
    border-color: #fc466b;
    background: linear-gradient(135deg, #232a37, #34495e);
    box-shadow: 0 0 10px #fc466b60;
    transform: scale(1.02);
  }
`;

const SaveBtn = styled.button`
  background: linear-gradient(90deg, #27ae60, #2ecc71);
  border: none;
  color: #fff;
  padding: 1.1rem 2.2rem;
  border-radius: 14px;
  cursor: pointer;
  font-weight: bold;
  font-size: 1.14rem;
  margin-top: 1.4rem;
  display: flex;
  align-items: center;
  gap: 0.65rem;
  transition: 0.16s;
  &:hover {
    background: linear-gradient(90deg, #2ecc71, #27ae60);
    transform: translateY(-3px);
    box-shadow: 0 6px 20px #27ae6044;
  }
`;

const ContextCell = styled.div`
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
  max-width: 280px;
`;

const ContextChip = styled.span`
  padding: 0.35rem 0.8rem;
  border-radius: 7px;
  font-size: 0.9rem;
  white-space: nowrap;
  font-weight: 500;
  background: ${({ missing }) =>
    missing
      ? "linear-gradient(90deg, #e74c3c, #c0392b)"
      : "linear-gradient(90deg, #27ae60, #2ecc71)"};
  color: #fff;
  border: 1.5px solid
    ${({ missing }) => (missing ? "#e74c3c" : "#27ae60")};
  &:hover {
    transform: scale(1.07);
  }
`;

const StatsBar = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-top: 1.5rem;
  padding: 1.1rem;
  background: #232b43;
  border-radius: 12px;
  border: 1px solid #3f5efb33;
`;

const StatItem = styled.div`
  text-align: center;
  .number {
    font-size: 1.6rem;
    font-weight: bold;
    color: #3498db;
  }
  .label {
    font-size: 1rem;
    color: #bdc3c7;
    margin-top: 0.2rem;
  }
`;

// --- Main Component ---
export default function CleanPage({ setStep, data, setData }) {
  const [opts, setOpts] = useState({
    dropMissing: false,
    fillWithMean: false,
    dropDuplicates: true
  });

  const [showMissingModal, setShowMissingModal] = useState(false);
  const [missingData, setMissingData] = useState([]);
  const [editedValues, setEditedValues] = useState({});

  function toggleOpt(key) {
    setOpts(o => ({ ...o, [key]: !o[key] }));
  }

  // Find all missing values with their positions
  useEffect(() => {
    if (!data || data.length === 0) return;
    const missing = [];
    const headers = data[0];
    for (let rowIndex = 1; rowIndex < data.length; rowIndex++) {
      const row = data[rowIndex];
      for (let colIndex = 0; colIndex < row.length; colIndex++) {
        const cell = row[colIndex];
        if (cell === "" || cell === null || cell === undefined) {
          missing.push({
            rowIndex,
            colIndex,
            header: headers[colIndex] || `Column ${colIndex + 1}`,
            rowData: row,
            key: `${rowIndex}-${colIndex}`
          });
        }
      }
    }
    setMissingData(missing);
  }, [data]);

  // Calculate data statistics
  const missingCount = data
    ? data.reduce(
        (count, row) =>
          count +
          row.filter(
            cell => cell === "" || cell === null || cell === undefined
          ).length,
        0
      )
    : 0;

  const totalCells =
    data && data.length > 0 ? data.length * data[0].length : 0;
  const filledCells = totalCells - missingCount;

  const handleInputChange = (key, value) => {
    setEditedValues(prev => ({
      ...prev,
      [key]: value
    }));
  };

  const saveManualEntries = () => {
    if (!data || !setData) return;
    const newData = [...data];
    Object.entries(editedValues).forEach(([key, value]) => {
      if (value.trim() !== "") {
        const [rowIndex, colIndex] = key.split("-").map(Number);
        newData[rowIndex][colIndex] = value.trim();
      }
    });
    setData(newData);
    setEditedValues({});
    setShowMissingModal(false);
  };

  const filledValuesCount = Object.values(editedValues).filter(
    val => val.trim() !== ""
  ).length;

  return (
    <Wrapper>
      <CleanBox>
        <SectionTitle>
          <FaBroom />
          Data Cleaning Options
        </SectionTitle>

        <DataStatus>
          <h4>📊 Data Status</h4>
          <p>• Total rows: {data ? data.length : 0}</p>
          <p>• Missing values: {missingCount}</p>
          <p>
            • Completion rate:{" "}
            {totalCells > 0
              ? Math.round((filledCells / totalCells) * 100)
              : 0}
            %
          </p>
          {missingCount > 0 && (
            <FillMissingBtn onClick={() => setShowMissingModal(true)}>
              <FaEdit />
              Fill Missing Values Manually ({missingCount})
            </FillMissingBtn>
          )}
        </DataStatus>

        <div style={{ marginTop: "1.7rem" }}>
          <h3
            style={{
              color: "#ffe082",
              marginBottom: "1.2rem",
              fontSize: "1.22rem",
              fontWeight: "700"
            }}
          >
            Automated Cleaning Options:
          </h3>

          <Option>
            <input
              type="checkbox"
              checked={opts.dropMissing}
              onChange={() => toggleOpt("dropMissing")}
            />
            <label>Drop rows with missing values</label>
          </Option>
          <Option>
            <input
              type="checkbox"
              checked={opts.fillWithMean}
              onChange={() => toggleOpt("fillWithMean")}
            />
            <label>Fill numeric missing values with mean</label>
          </Option>
          <Option>
            <input
              type="checkbox"
              checked={opts.dropDuplicates}
              onChange={() => toggleOpt("dropDuplicates")}
            />
            <label>Remove duplicate rows</label>
          </Option>
        </div>

        <div
          style={{
            color: "#ffe082",
            marginTop: "2rem",
            fontWeight: 600,
            fontSize: "1.07rem"
          }}
        >
          {missingCount > 0 ? (
            <>
              <FaEye style={{ marginRight: "0.5rem" }} />
              {missingCount} missing values remain!
              <br />
              <span style={{ color: "#fff", fontWeight: 400 }}>
                Fill them manually for best results or use automated cleaning.
              </span>
            </>
          ) : (
            <>
              <FaCheck style={{ marginRight: "0.5rem", color: "#27ae60" }} />
              All values present!<br />
              <span style={{ color: "#fff", fontWeight: 400 }}>
                Data ready for preprocessing!
              </span>
            </>
          )}
        </div>
      </CleanBox>

      <ButtonGroup>
        <BackBtn onClick={() => setStep(3)}>← Back to Edit</BackBtn>
        <NextBtn onClick={() => setStep(5)}>
          Next: Preprocessing →
        </NextBtn>
      </ButtonGroup>

      {showMissingModal && (
        <ModalBackdrop
          onClick={e =>
            e.target === e.currentTarget && setShowMissingModal(false)
          }
        >
          <ModalContent>
            <ModalHeader>
              <h3>
                <FaEdit />
                Fill Missing Values
              </h3>
              <CloseBtn onClick={() => setShowMissingModal(false)}>
                <FaTimes />
                Close
              </CloseBtn>
            </ModalHeader>
            <p
              style={{
                color: "#ffe082",
                marginBottom: "1.25rem",
                fontSize: "1.1rem"
              }}
            >
              {missingData.length > 0
                ? `There are ${missingData.length} missing values. Fill below:`
                : "No missing values!"}
            </p>

            <StatsBar>
              <StatItem>
                <div className="number">{missingData.length}</div>
                <div className="label">Missing</div>
              </StatItem>
              <StatItem>
                <div className="number">{filledValuesCount}</div>
                <div className="label">Entered</div>
              </StatItem>
              <StatItem>
                <div className="number">
                  {missingData.length - filledValuesCount}
                </div>
                <div className="label">Remaining</div>
              </StatItem>
            </StatsBar>

            {missingData.length > 0 && (
              <>
                <MissingDataTable>
                  <thead>
                    <tr>
                      <th>Row</th>
                      <th>Column</th>
                      <th>Context</th>
                      <th>Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {missingData.map(item => (
                      <tr key={item.key}>
                        <td>
                          <strong style={{ color: "#3498db" }}>
                            #{item.rowIndex}
                          </strong>
                        </td>
                        <td>
                          <strong style={{ color: "#e67e22" }}>
                            {item.header}
                          </strong>
                        </td>
                        <td>
                          <ContextCell>
                            {item.rowData.slice(0, 4).map((cell, i) => (
                              <ContextChip key={i} missing={!cell}>
                                {cell || "MISSING"}
                              </ContextChip>
                            ))}
                            {item.rowData.length > 4 && (
                              <span
                                style={{
                                  color: "#bdc3c7",
                                  fontSize: "0.92rem"
                                }}
                              >
                                ...+{item.rowData.length - 4} more
                              </span>
                            )}
                          </ContextCell>
                        </td>
                        <td>
                          <MissingInput
                            type="text"
                            placeholder="Enter value..."
                            value={editedValues[item.key] || ""}
                            onChange={e =>
                              handleInputChange(item.key, e.target.value)
                            }
                          />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </MissingDataTable>
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center"
                  }}
                >
                  <SaveBtn onClick={saveManualEntries}>
                    <FaCheck />
                    Save All Changes ({filledValuesCount})
                  </SaveBtn>
                  {filledValuesCount > 0 && (
                    <p
                      style={{
                        color: "#2ecc71",
                        fontSize: "1rem",
                        fontWeight: "bold"
                      }}
                    >
                      ✓ {filledValuesCount} ready to save
                    </p>
                  )}
                </div>
              </>
            )}
          </ModalContent>
        </ModalBackdrop>
      )}
    </Wrapper>
  );
}