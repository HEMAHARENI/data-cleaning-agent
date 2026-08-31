import React, { useEffect } from "react";
import styled from "styled-components";
import { FaTable } from "react-icons/fa";

const Wrapper = styled.div`
  min-height: 100vh;
  background: linear-gradient(135deg, #181e41 40%, #3f5efb 100%);
  padding: 2.5rem 0;
`;

const PreviewBox = styled.div`
  background: rgba(39, 47, 89, 0.7);
  border-radius: 28px;
  padding: 2.5rem 2rem;
  box-shadow: 0 6px 44px 0 #3f5efb50;
  margin: 2.5rem auto;
  max-width: 760px;
  border: 1px solid #383d5a;
`;

const SectionTitle = styled.h2`
  font-weight: 800;
  font-size: 2.1rem;
  letter-spacing: 0.01em;
  margin-bottom: 1.2rem;
  color: #ffe082;
  display: flex;
  align-items: center;
  gap: 1.1rem;
`;

const Table = styled.table`
  width: 100%;
  border-collapse: collapse;
  margin-top: 1.3rem;
  thead {
    background: linear-gradient(90deg,#fc466b22,#3f5efb22);
  }
  th, td {
    padding: 10px 7px;
    border-bottom: 1.5px solid #fc466b44;
    text-align: left;
    font-size: 1.03rem;
  }
  th {
    color: #fc466b;
    font-weight: bold;
    text-transform: uppercase;
  }
  td {
    color: #fff;
    font-size: 1rem;
  }
`;

const NextBtn = styled.button`
  background: linear-gradient(90deg, #fc466b, #ffe082 100%);
  border: none;
  padding: 1rem 2.8rem;
  border-radius: 36px;
  font-size: 1.18rem;
  font-weight: bold;
  color: #3f5efb;
  margin-top: 2.7rem;
  cursor: pointer;
  box-shadow: 0 2px 14px #fc466b35;
  transition: 0.2s;
  &:hover {
    background: linear-gradient(90deg, #3f5efb, #fc466b 100%);
    color: #ffe082;
    box-shadow: 0 4px 16px #3f5efb44;
  }
`;

const ManualEditBtn = styled.button`
  background: linear-gradient(90deg, #9c27b0, #fc466b 100%);
  border: none;
  padding: 1rem 2.6rem;
  border-radius: 32px;
  font-size: 1.17rem;
  font-weight: bold;
  color: #fff;
  margin-top: 2.7rem;
  margin-right: 1.3rem;
  cursor: pointer;
  box-shadow: 0 2px 14px #9c27b033;
  &:hover {
    background: linear-gradient(90deg, #fc466b, #9c27b0 100%);
  }
`;

const ButtonGroup = styled.div`
  display: flex;
  align-items: center;
`;

export default function PreviewPage({ setStep, fileData, data, setData }) {
  const columns = ["Name", "Age", "City", "Score"];

  useEffect(() => {
    if (!data) {
      const demoData = [
        ["Ava", 23, "Paris", 91],
        ["Leo", 27, "Berlin", 84],
        ["Maya", 21, "Tokyo", ""],
        ["Tom", null, "London", 77],
        ["Amy", 25, "Paris", 92]
      ];
      setData(demoData);
    }
  }, [data, setData]);

  const hasMissingValues = data && data.some(row =>
    row.some(cell => cell === "" || cell === null || cell === undefined)
  );

  const missingCount = data ? data.reduce((count, row) =>
    count + row.filter(cell => cell === "" || cell === null || cell === undefined).length, 0
  ) : 0;

  return (
    <Wrapper>
      <PreviewBox>
        <SectionTitle>
          <FaTable />
          Data Preview
        </SectionTitle>
        <Table>
          <thead>
            <tr>
              {columns.map(col => <th key={col}>{col}</th>)}
            </tr>
          </thead>
          <tbody>
            {data && data.map((row, i) => (
              <tr key={i}>
                {row.map((cell, j) => (
                  <td
                    key={j}
                    style={{
                      color: cell === "" || cell === null || cell === undefined ? "#ffe082" : "#fff"
                    }}
                  >
                    {cell === "" || cell === null || cell === undefined ? "(missing)" : cell}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </Table>
        {hasMissingValues && (
          <div style={{ marginTop: '1.2rem', color: '#ffe082', fontWeight: 700, fontSize: "1.07rem" }}>
            ⚠️ {missingCount} missing values detected!
          </div>
        )}
      </PreviewBox>
      <ButtonGroup>
        {hasMissingValues && (
          <ManualEditBtn onClick={() => setStep(3)}>
            ✏️ Edit Missing Values
          </ManualEditBtn>
        )}
        <NextBtn onClick={() => setStep(4)}>
          {hasMissingValues ? "Skip to Cleaning →" : "Next →"}
        </NextBtn>
      </ButtonGroup>
    </Wrapper>
  );
}