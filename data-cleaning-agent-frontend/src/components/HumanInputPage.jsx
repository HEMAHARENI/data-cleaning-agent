import React, { useState } from "react";
import styled from "styled-components";
import { FaEdit } from "react-icons/fa";

const Wrapper = styled.div`
  min-height: 100vh;
  background: linear-gradient(135deg, #181e41 40%, #9c27b0 100%);
  padding: 2.5rem 0;
`;

const InputBox = styled.div`
  background: rgba(51, 33, 61, 0.7);
  border-radius: 28px;
  padding: 2.5rem 2rem;
  box-shadow: 0 6px 44px 0 #9c27b066;
  margin: 2.5rem auto;
  max-width: 760px;
  border: 1px solid #5e3576;
`;

const Title = styled.h2`
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
  margin-top: 1.5rem;
  background: rgba(255,255,255,0.01);

  th, td {
    padding: 10px 8px;
    border-bottom: 1.5px solid #9c27b044;
    text-align: left;
    font-size: 1.04rem;
  }
  th {
    color: #ffe082;
    font-weight: bold;
    background: linear-gradient(90deg, #9c27b022, #fc466b22);
    text-transform: uppercase;
  }
`;

const EditableCell = styled.input`
  width: 100%;
  padding: 0.58rem;
  border-radius: 9px;
  border: 2px solid ${props => props.isMissing ? '#ffe082' : '#666'};
  background: ${props => props.isMissing ? '#ffe08222' : '#ffffff14'};
  color: ${props => props.isMissing ? '#ffe082' : '#fff'};
  font-size: 1rem;
  outline: none;
  transition: border-color 0.2s, background 0.2s;
  &:focus { border-color: #fc466b; background: #fc466b11; color: #fff;}
  &::placeholder {
    color: ${props => props.isMissing ? '#ffe08288' : '#ffffff70'};
    font-style: italic;
  }
`;

const ButtonGroup = styled.div`
  display: flex;
  gap: 1.2rem;
  margin-top: 2.5rem;
`;

const SkipBtn = styled.button`
  background: linear-gradient(90deg, #444, #888);
  border: none;
  padding: 1rem 2.1rem;
  border-radius: 32px;
  font-size: 1.08rem;
  font-weight: bold;
  color: #fff;
  cursor: pointer;
  box-shadow: 0 2px 10px #5e357633;
  &:hover {
    background: linear-gradient(90deg, #888, #aaa);
  }
`;

const SaveBtn = styled.button`
  background: linear-gradient(90deg, #9c27b0, #fc466b 100%);
  border: none;
  padding: 1rem 2.6rem;
  border-radius: 32px;
  font-size: 1.2rem;
  font-weight: bold;
  color: #fff;
  cursor: pointer;
  box-shadow: 0 2px 14px #9c27b033;
  &:hover {
    background: linear-gradient(90deg, #fc466b, #9c27b0 100%);
  }
`;

const TipsBox = styled.div`
  margin-top: 1.7rem;
  padding: 1.1rem;
  background: #3f5efb20;
  border-radius: 12px;
  h3 {
    color: #3f5efb;
    font-weight: bold;
    margin-bottom: 0.7rem;
    font-size: 1.07rem;
  }
  ul {
    color: #fff;
    font-size: 1rem;
    margin: 0;
    padding-left: 1.4rem;
    li { margin: 0.3rem 0; }
  }
`;

export default function HumanInputPage({ setStep, data, setData, columns = ["Name", "Age", "City", "Score"] }) {
  const [editableData, setEditableData] = useState(data || [
    ["Ava", 23, "Paris", 91],
    ["Leo", 27, "Berlin", 84],
    ["Maya", 21, "Tokyo", ""],
    ["Tom", null, "London", 77],
    ["Amy", 25, "Paris", 92]
  ]);

  const handleCellChange = (rowIndex, colIndex, value) => {
    const newData = [...editableData];
    if (columns[colIndex] === "Age" || columns[colIndex] === "Score") {
      const numValue = value === "" ? null : Number(value);
      newData[rowIndex][colIndex] = isNaN(numValue) ? value : numValue;
    } else {
      newData[rowIndex][colIndex] = value === "" ? null : value;
    }
    setEditableData(newData);
  };

  const handleSave = () => {
    setData(editableData);
    setStep(4);
  };

  const handleSkip = () => {
    setStep(4);
  };

  const missingCount = editableData.reduce((count, row) =>
    count + row.filter(cell => cell === "" || cell === null || cell === undefined).length, 0
  );

  return (
    <Wrapper>
      <InputBox>
        <Title>
          <FaEdit style={{color:'#9c27b0',verticalAlign:'middle'}}/>
          Manual Data Entry
        </Title>
        <p style={{color:'#ffe082', marginBottom:'1.5rem', fontSize:'1.08rem'}}>
          Fill in missing values manually or leave them for automatic processing.<br />
          <span style={{color:'#fff'}}>Missing values remaining: <strong>{missingCount}</strong></span>
        </p>
        <Table>
          <thead>
            <tr>
              {columns.map(col => (
                <th key={col}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {editableData.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {row.map((cell, colIndex) => {
                  const isMissing = cell === "" || cell === null || cell === undefined;
                  return (
                    <td key={colIndex}>
                      <EditableCell
                        type="text"
                        value={cell === null || cell === undefined ? "" : cell}
                        onChange={(e) => handleCellChange(rowIndex, colIndex, e.target.value)}
                        isMissing={isMissing}
                        placeholder={isMissing ? "Enter value..." : ""}
                      />
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </Table>
        <TipsBox>
          <h3>💡 Tips:</h3>
          <ul>
            <li>Yellow highlighted cells have missing values</li>
            <li>Leave cells empty for automatic processing</li>
            <li>For Age/Score columns, use numbers only</li>
            <li>You can always go back and modify later</li>
          </ul>
        </TipsBox>
      </InputBox>
      <ButtonGroup>
        <SkipBtn onClick={handleSkip}>Skip Manual Entry</SkipBtn>
        <SaveBtn onClick={handleSave}>Save & Continue →</SaveBtn>
      </ButtonGroup>
    </Wrapper>
  );
}