<?php

namespace common\jobs;

use common\models\User;
use common\models\Employee;
use common\components\Auth;
use common\models\StatusReason;
use PhpOffice\PhpSpreadsheet\Cell\Coordinate;
use PhpOffice\PhpSpreadsheet\Spreadsheet;
use PhpOffice\PhpSpreadsheet\Writer\Xlsx;
use yii\base\BaseObject;
use yii\helpers\ArrayHelper;
use yii\queue\JobInterface;

class EmployeeListReportJob extends BaseObject implements JobInterface
{
    public $jobId;
    public $userId;

    public function execute($queue)
    {
        $actor = User::findOne($this->userId);

        $headers = [
            'Employee ID',
            'Status',
            'First Name',
            'Last Name',
            'Date of Birth',
            'Address',
            'Address2',
            'City',
            'State',
            'Zip',
            'Mobile',
            'Home',
            'Email',
            'Transportation',
            'Date of Background',
            'No Background',
            'Start Date',
            'Number of Shifts Worked',
            'Language',
            'Certifications',
            'Rehire Date',
            'Recruited By',
            'Referred By',
            'Positions',
            'County of Residence',
            'Concierge Date',
        ];

        if ($actor && Auth::isRole($actor, [Auth::ROLE_OWNER, Auth::ROLE_SUPER_ADMIN, Auth::ROLE_HUMAN_RESOURCE])) {
            $headers = [
                'Employee ID',
                'Status',
                'First Name',
                'Last Name',
                'Date of Birth',
                'Address',
                'Address2',
                'City',
                'State',
                'Zip',
                'Mobile',
                'Home',
                'Email',
                'Gender',
                'Transportation',
                'Date of Background',
                'No Background',
                'Start Date',
                'Number of Shifts Worked',
                'Language',
                'Certifications',
                'SS Number',
                'Rehire Date',
                'Recruited By',
                'Referred By',
                'Positions',
                'County of Residence',
                'Backgrounds',
                'Concierge Date',
            ];
        }

        $indicesByHeader = array_map(function ($arrayIndex) {
            return $arrayIndex + 1;
        }, array_flip($headers));

        $columnNamesByHeader = [];
        foreach ($headers as $header) {
            $columnNamesByHeader[$header] = Coordinate::stringFromColumnIndex($indicesByHeader[$header]);
        }

        $lastColumnIndex = count($headers);
        $lastColumnName = Coordinate::stringFromColumnIndex($lastColumnIndex);

        $spreadsheet = new Spreadsheet();
        $sheet = $spreadsheet->getActiveSheet();

        $bold = [
            'font' => [
                'bold' => true,
            ],
        ];
        $sheet->getStyle("A1:{$lastColumnName}1")->applyFromArray($bold);

        foreach (range(1, $lastColumnIndex) as $columnIndex) {
            $columnID = \PhpOffice\PhpSpreadsheet\Cell\Coordinate::stringFromColumnIndex($columnIndex);

            $sheet->getColumnDimension($columnID)->setAutoSize(true);
        }

        foreach ($headers as $header) {
            $columnName = $columnNamesByHeader[$header];
            $sheet->setCellValue("{$columnName}1", $header);
        }

        $employees = Employee::find()
            ->andWhere(['!=', 'status', Employee::STATUS_CANDIDATE])
            ->orderBy('first_name')
            ->all();
        $i = 2;

        foreach ($employees as $e) {
            /* @var $e Employee */

            if ($e->transportation == 1) {
                $transportation = 'Car';
            } elseif ($e->transportation == 2) {
                $transportation = 'Motorcycle';
            } elseif ($e->transportation == 3) {
                $transportation = 'Public Transit';
                # code...
            } else {
                $transportation = '';
            }

            if (Employee::STATUS_OTHER == $e->status) {
                $statusReason = $e->status_reason ? StatusReason::findOne(['id' => $e->status_reason]) : null;
                $status = $statusReason ? $statusReason->reason : 'Other';
            } else {
                $status = ArrayHelper::getValue(Employee::getStatusLabels(), $e->status, 'Other');
            }

            $sheet->setCellValue($columnNamesByHeader['Employee ID'] . $i, $e->payroll_id);
            $sheet->setCellValue($columnNamesByHeader['Status'] . $i, $status);
            $sheet->setCellValue($columnNamesByHeader['First Name'] . $i, $e->first_name);
            $sheet->setCellValue($columnNamesByHeader['Last Name'] . $i, $e->last_name);
            $sheet->setCellValue($columnNamesByHeader['Date of Birth'] . $i, ($e->dob ? date('m/d/Y', strtotime($e->dob)) : ''));
            $sheet->setCellValue($columnNamesByHeader['Address'] . $i, $e->address1);
            $sheet->setCellValue($columnNamesByHeader['Address2'] . $i, $e->address2);
            $sheet->setCellValue($columnNamesByHeader['City'] . $i, $e->city);
            $sheet->setCellValue($columnNamesByHeader['State'] . $i, $e->state);
            $sheet->setCellValue($columnNamesByHeader['Zip'] . $i, $e->zip);
            $sheet->setCellValue($columnNamesByHeader['Mobile'] . $i, $e->mobile);
            $sheet->setCellValue($columnNamesByHeader['Home'] . $i, $e->home);
            $sheet->setCellValue($columnNamesByHeader['Email'] . $i, $e->email);
            $sheet->setCellValue($columnNamesByHeader['Transportation'] . $i, $transportation);
            $sheet->setCellValue($columnNamesByHeader['Date of Background'] . $i, $e->background_date);
            $sheet->setCellValue($columnNamesByHeader['Concierge Date'] . $i, $e->concierge_date);

            if (empty($e->background)) {
                $background_check
                    = in_array($e->background_query, [Employee::BACKGROUND_QUERY_PENDING, Employee::BACKGROUND_QUERY_REQUESTED])
                        ? ArrayHelper::getValue(Employee::getBackgroundQueryLabels(), $e->background_query)
                        : 'X';
            } else {
                $background_check = '';
            }

            $sheet->setCellValue($columnNamesByHeader['No Background'] . $i, $background_check);
            $sheet->setCellValue($columnNamesByHeader['Start Date'] . $i, date("m/d/Y", strtotime($e->start_date)));
            $sheet->setCellValue($columnNamesByHeader['Number of Shifts Worked'] . $i, $e->getNumberOfShiftsWorked());

            if ($e->languages) {
                $languages = join(', ', ArrayHelper::map($e->languages, 'language', 'language'));
            } else {
                $languages = '';
            }

            $sheet->setCellValue($columnNamesByHeader['Language'] . $i, $languages);

            $certifications = [];

            foreach ($e->employeeCertifications as $employeeCertification) {
                $certifications[] = $employeeCertification->certification->name;
            }

            $sheet->setCellValue($columnNamesByHeader['Certifications'] . $i, join(', ', $certifications));

            $ssn = $e->ssn;
            $ssn = trim(str_replace('-', '', $ssn));
            $ssn
                = join('-', [
                substr($ssn, 0, 3),
                substr($ssn, 3, 2),
                substr($ssn, 5, 4),
            ]);

            $sheet->setCellValue($columnNamesByHeader['Rehire Date'] . $i, $e->start_date2 ? date("m/d/Y", strtotime($e->start_date2)) : '');
            $sheet->setCellValue($columnNamesByHeader['Recruited By'] . $i, $e->recruitedBy ? $e->recruitedBy->getFullname() : '');

            $sheet->setCellValue(
                $columnNamesByHeader['Referred By'] . $i,
                (function () use (&$e) {
                    $referredBy = $e->getReferredBy();
                    if ($referredBy) {
                        return $referredBy->name;
                    }

                    return '-';
                })()
            );

            $sheet->setCellValue(
                $columnNamesByHeader['Positions'] . $i,
                (join(', ', array_map(function (\common\models\Position $position) {
                    return $position->description;
                }, $e->getEligiblePositions()->all())))
            );

            $sheet->setCellValue(
                $columnNamesByHeader['County of Residence'] . $i,
                ArrayHelper::getValue($e, 'county.name')
            );

            if ($actor && Auth::isRole($actor, [Auth::ROLE_OWNER, Auth::ROLE_SUPER_ADMIN, Auth::ROLE_HUMAN_RESOURCE])) {
                $sheet->setCellValue($columnNamesByHeader['Gender'] . $i, $e->sex);
                $sheet->setCellValue($columnNamesByHeader['SS Number'] . $i, $ssn);
                $sheet->setCellValue(
                    $columnNamesByHeader['Backgrounds'] . $i,
                    (function () use (&$e) {
                        $names = [];
                        foreach ($e->getEmployeeBackgrounds()->with('background')->each() as $employeeBackground) {
                            /* @var $employeeBackground \common\models\EmployeeBackground */
                            $names[] = $employeeBackground->background->name;
                        }

                        return join(', ', $names);
                    })()
                );
            }

            $i++;
        }

        $filename = 'employee-list' . time() . '.xlsx';

        $saveLocation = \Yii::getAlias("@api/web/temp/{$filename}");
        $writer = new Xlsx($spreadsheet);
        $writer->save($saveLocation);

        $jobModel = \common\models\Job::findOne($this->jobId);
        $jobModel->end(['filename' => $filename]);

        $spreadsheet->disconnectWorksheets();
        unset($spreadsheet);
    }
}